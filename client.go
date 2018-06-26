package capi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Client struct {
	addr      string
	appGuid   string
	spaceGuid string
	doer      Doer
}

type Doer interface {
	Do(req *http.Request) (*http.Response, error)
}

func NewClient(addr, appGuid, spaceGuid string, d Doer) *Client {
	// Replace HTTPS with HTTP so the HTTP_PROXY can do the work for us
	addr = strings.Replace(addr, "https", "http", 1)

	return &Client{
		doer:      d,
		addr:      addr,
		appGuid:   appGuid,
		spaceGuid: spaceGuid,
	}
}

type HealthCheck struct {
	Type string `json:"type"`
	Data struct {
		Timeout           int    `json:"timeout"`
		InvocationTimeout int    `json:"invocation_timeout"`
		Endpoint          string `json:"endpoint"`
	} `json:"data"`
}

type Links struct {
	Href   string `json:"href"`
	Method string `json:"method"`
}

type Process struct {
	Type        string           `json:"type"`
	Command     string           `json:"command"`
	Instances   int              `json:"instances"`
	MemoryInMB  int              `json:"memory_in_mb"`
	DiskInMB    int              `json:"disk_in_mb"`
	HealthCheck HealthCheck      `json:"health_check"`
	Guid        string           `json:"guid"`
	CreatedAt   time.Time        `json:"created_at"`
	UpdatedAt   time.Time        `json:"updated_at"`
	Links       map[string]Links `json:"links"`
}

type ProcessStats struct {
	Type  string `json:"type"`
	Index int    `json:"index"`
	State string `json:"state"`
	Usage struct {
		Time time.Time `json:"time"`
		CPU  float64   `json:"cpu"`
		Mem  float64   `json:"mem"`
		Disk int       `json:"disk"`
	} `json:"usage"`
	Host      string `json:"host"`
	Uptime    int    `json:"uptime"`
	MemQuota  int    `json:"mem_quota"`
	DiskQuota int    `json:"disk_quota"`
	FdsQuota  int    `json:"fds_quota"`
}

type Task struct {
	SequenceID  int              `json:"sequence_id"`
	Name        string           `json:"name"`
	Command     string           `json:"command"`
	DiskInMB    int              `json:"disk_in_mb"`
	MemoryInMB  int              `json:"memory_in_mb"`
	State       string           `json:"state"`
	DropletGuid string           `json:"droplet_guid"`
	Guid        string           `json:"guid"`
	CreatedAt   time.Time        `json:"created_at"`
	UpdatedAt   time.Time        `json:"updated_at"`
	Links       map[string]Links `json:"links"`
}

func (c *Client) Processes(ctx context.Context, appGuid string) ([]Process, error) {
	addr := c.addr
	var processes []Process

	for {
		u, err := url.Parse(addr)
		if err != nil {
			return nil, err
		}
		u.Path = fmt.Sprintf("/v3/apps/%s/processes", appGuid)

		req := &http.Request{
			URL:    u,
			Method: "GET",
			Header: http.Header{},
		}
		req = req.WithContext(ctx)

		resp, err := c.doer.Do(req)
		if err != nil {
			return nil, err
		}

		defer func() {
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
		}()

		if resp.StatusCode != 200 {
			data, _ := ioutil.ReadAll(resp.Body)
			return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, data)
		}

		var results struct {
			Pagination struct {
				Next struct {
					Href string `json:"href"`
				} `json:"next"`
			} `json:"pagination"`
			Resources []Process `json:"resources"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
			return nil, err
		}

		// Replace HTTPS with HTTP so the HTTP_PROXY can do the work for us
		results.Pagination.Next.Href = strings.Replace(results.Pagination.Next.Href, "https", "http", 1)

		for _, t := range results.Resources {
			processes = append(processes, t)
		}

		if results.Pagination.Next.Href != "" {
			addr = results.Pagination.Next.Href
			continue
		}

		return processes, nil
	}
}

func (c *Client) ProcessStats(ctx context.Context, processGuid string) ([]ProcessStats, error) {
	addr := c.addr
	var stats []ProcessStats

	for {
		u, err := url.Parse(addr)
		if err != nil {
			return nil, err
		}
		u.Path = fmt.Sprintf("/v3/processes/%s/stats", processGuid)

		req := &http.Request{
			URL:    u,
			Method: "GET",
			Header: http.Header{},
		}
		req = req.WithContext(ctx)

		resp, err := c.doer.Do(req)
		if err != nil {
			return nil, err
		}

		defer func() {
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
		}()

		if resp.StatusCode != 200 {
			data, _ := ioutil.ReadAll(resp.Body)
			return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, data)
		}

		var results struct {
			Pagination struct {
				Next struct {
					Href string `json:"href"`
				} `json:"next"`
			} `json:"pagination"`
			Resources []ProcessStats `json:"resources"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
			return nil, err
		}

		// Replace HTTPS with HTTP so the HTTP_PROXY can do the work for us
		results.Pagination.Next.Href = strings.Replace(results.Pagination.Next.Href, "https", "http", 1)

		for _, t := range results.Resources {
			stats = append(stats, t)
		}

		if results.Pagination.Next.Href != "" {
			addr = results.Pagination.Next.Href
			continue
		}

		return stats, nil
	}
}

func (c *Client) GetAppGuid(ctx context.Context, appName string) (string, error) {
	u, err := url.Parse(fmt.Sprintf("%s/v2/apps?q=name%%3A%s&q=space_guid%%3A%s", c.addr, appName, c.spaceGuid))
	if err != nil {
		return "", err
	}

	req := &http.Request{
		URL:    u,
		Method: "GET",
		Header: http.Header{
			"Accept": []string{"application/json"},
		},
	}
	req = req.WithContext(ctx)

	resp, err := c.doer.Do(req)
	if err != nil {
		return "", err
	}

	defer func(resp *http.Response) {
		// Fail safe to ensure the clients are being cleaned up
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}(resp)

	if resp.StatusCode != http.StatusOK {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return "", err
		}

		return "", fmt.Errorf("unexpected response %d: %s", resp.StatusCode, data)
	}

	var result struct {
		Resources []struct {
			MetaData struct {
				Guid string `json:"guid"`
			} `json:"metadata"`
		} `json:"resources"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}

	if len(result.Resources) == 0 {
		return "", errors.New("empty results")
	}

	return result.Resources[0].MetaData.Guid, nil
}

func (c *Client) GetDropletGuid(ctx context.Context, appGuid string) (string, error) {
	u, err := url.Parse(fmt.Sprintf("%s/v3/apps/%s/droplets/current", c.addr, appGuid))
	if err != nil {
		return "", err
	}

	req := &http.Request{
		URL:    u,
		Method: "GET",
		Header: http.Header{
			"Accept": []string{"application/json"},
		},
	}
	req = req.WithContext(ctx)

	resp, err := c.doer.Do(req)
	if err != nil {
		return "", err
	}

	defer func(resp *http.Response) {
		// Fail safe to ensure the clients are being cleaned up
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}(resp)

	if resp.StatusCode != http.StatusOK {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return "", err
		}

		return "", fmt.Errorf("unexpected response %d: %s", resp.StatusCode, data)
	}

	var result struct {
		Guid string `json:"guid"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}

	if result.Guid == "" {
		return "", errors.New("empty results")
	}

	return result.Guid, nil
}

func (c *Client) CreateTask(ctx context.Context, command string, interval time.Duration) error {
	u, err := url.Parse(c.addr)
	if err != nil {
		return err
	}
	u.Path = fmt.Sprintf("/v3/apps/%s/tasks", c.appGuid)

	marshalled, err := json.Marshal(struct {
		Command     string `json:"command"`
		DropletGuid string `json:"droplet_guid,omitempty"`
	}{
		Command: command,
	})
	if err != nil {
		return err
	}

	req := &http.Request{
		URL:    u,
		Method: "POST",
		Body:   ioutil.NopCloser(bytes.NewReader(marshalled)),
		Header: http.Header{
			"Content-Type": []string{"application/json"},
		},
	}
	req = req.WithContext(ctx)

	resp, err := c.doer.Do(req)
	if err != nil {
		return err
	}

	defer func(resp *http.Response) {
		// Fail safe to ensure the clients are being cleaned up
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}(resp)

	if resp.StatusCode != 202 {
		data, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, data)
	}

	for {
		var results struct {
			State string `json:"state"`
			Links struct {
				Self struct {
					Href string `json:"href"`
				} `json:"self"`
			} `json:"links"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
			return err
		}

		// Replace HTTPS with HTTP so the HTTP_PROXY can do the work for us
		results.Links.Self.Href = strings.Replace(results.Links.Self.Href, "https", "http", 1)

		resp.Body.Close()

		switch results.State {
		case "RUNNING":
			time.Sleep(interval)

			u, err := url.Parse(results.Links.Self.Href)
			if err != nil {
				return err
			}

			req := &http.Request{
				URL:    u,
				Method: "GET",
				Header: http.Header{},
			}
			req = req.WithContext(ctx)

			resp, err = c.doer.Do(req)
			if err != nil {
				return err
			}

			defer func(resp *http.Response) {
				// Fail safe to ensure the clients are being cleaned up
				io.Copy(ioutil.Discard, resp.Body)
				resp.Body.Close()
			}(resp)

			continue
		case "FAILED":
			return errors.New("task failed")
		default:
			return nil
		}
	}

	return nil
}

func (c *Client) GetTask(ctx context.Context, guid string) (Task, error) {
	u, err := url.Parse(c.addr)
	if err != nil {
		return Task{}, err
	}
	u.Path = fmt.Sprintf("/v3/tasks/%s", guid)

	req := &http.Request{
		URL:    u,
		Method: "GET",
		Body:   ioutil.NopCloser(bytes.NewReader(nil)),
		Header: http.Header{
			"Accept": []string{"application/json"},
		},
	}
	req = req.WithContext(ctx)

	resp, err := c.doer.Do(req)
	if err != nil {
		return Task{}, err
	}

	defer func(resp *http.Response) {
		// Fail safe to ensure the clients are being cleaned up
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}(resp)

	if resp.StatusCode != 200 {
		data, _ := ioutil.ReadAll(resp.Body)
		return Task{}, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, data)
	}

	var task Task
	if err := json.NewDecoder(resp.Body).Decode(&task); err != nil {
		return Task{}, err
	}

	// Ensure all links are converted to http for proxy
	for k, l := range task.Links {
		l.Href = strings.Replace(l.Href, "https", "http", 1)

		if l.Method == "" {
			l.Method = "GET"
		}
		task.Links[k] = l
	}

	return task, nil
}

func (c *Client) RunTask(ctx context.Context, command, name, droplet, appGuid string) (Task, error) {
	if appGuid == "" {
		appGuid = c.appGuid
	}

	u, err := url.Parse(c.addr)
	if err != nil {
		return Task{}, err
	}
	u.Path = fmt.Sprintf("/v3/apps/%s/tasks", appGuid)

	marshalled, err := json.Marshal(struct {
		Command     string `json:"command"`
		Name        string `json:"name,omitempty"`
		DropletGuid string `json:"droplet_guid,omitempty"`
	}{
		Command:     command,
		Name:        name,
		DropletGuid: droplet,
	})
	if err != nil {
		return Task{}, err
	}

	req := &http.Request{
		URL:    u,
		Method: "POST",
		Body:   ioutil.NopCloser(bytes.NewReader(marshalled)),
		Header: http.Header{
			"Content-Type": []string{"application/json"},
		},
	}
	req = req.WithContext(ctx)

	resp, err := c.doer.Do(req)
	if err != nil {
		return Task{}, err
	}

	defer func(resp *http.Response) {
		// Fail safe to ensure the clients are being cleaned up
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}(resp)

	if resp.StatusCode != 202 {
		data, _ := ioutil.ReadAll(resp.Body)
		return Task{}, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, data)
	}

	var t Task
	if err := json.NewDecoder(resp.Body).Decode(&t); err != nil {
		return Task{}, err
	}

	// Ensure all links are converted to http for proxy
	for k, l := range t.Links {
		l.Href = strings.Replace(l.Href, "https", "http", 1)

		if l.Method == "" {
			l.Method = "GET"
		}
		t.Links[k] = l
	}

	return t, nil
}

func (c *Client) ListTasks(ctx context.Context, appGuid string, query map[string][]string) ([]Task, error) {
	var results []Task
	addr := c.addr

	for {
		u, err := url.Parse(addr)
		if err != nil {
			return nil, err
		}
		u.Path = fmt.Sprintf("/v3/apps/%s/tasks", appGuid)

		q := u.Query()
		for k, v := range query {
			for _, vv := range v {
				q.Add(k, vv)
			}
		}
		u.RawQuery = q.Encode()

		req := &http.Request{
			URL:    u,
			Method: "GET",
			Header: http.Header{},
		}
		req = req.WithContext(ctx)

		resp, err := c.doer.Do(req)
		if err != nil {
			return nil, err
		}

		defer func() {
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
		}()

		if resp.StatusCode != 200 {
			data, _ := ioutil.ReadAll(resp.Body)
			return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, data)
		}

		var tasks struct {
			Pagination struct {
				Next struct {
					Href string `json:"href"`
				} `json:"next"`
			} `json:"pagination"`
			Resources []Task `json:"resources"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
			return nil, err
		}

		results = append(results, tasks.Resources...)

		if tasks.Pagination.Next.Href != "" {
			addr = tasks.Pagination.Next.Href
			continue
		}

		return results, nil
	}
}

func (c *Client) GetPackageGuid(ctx context.Context, appGuid string) (guid, downloadAddr string, err error) {
	u, err := url.Parse(fmt.Sprintf("%s/v3/apps/%s/droplets/current", c.addr, appGuid))
	if err != nil {
		return "", "", err
	}

	req := &http.Request{
		URL:    u,
		Method: "GET",
		Header: http.Header{
			"Accept": []string{"application/json"},
		},
	}
	req = req.WithContext(ctx)

	resp, err := c.doer.Do(req)
	if err != nil {
		return "", "", err
	}

	defer func(resp *http.Response) {
		// Fail safe to ensure the clients are being cleaned up
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}(resp)

	if resp.StatusCode != http.StatusOK {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return "", "", err
		}

		return "", "", fmt.Errorf("unexpected response %d: %s", resp.StatusCode, data)
	}

	var result struct {
		Links struct {
			Package struct {
				Href string `json:"href"`
			} `json:"package"`
		} `json:"links"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", "", err
	}

	if result.Links.Package.Href == "" {
		return "", "", errors.New("empty results")
	}

	// Replace HTTPS with HTTP so the HTTP_PROXY can do the work for us
	result.Links.Package.Href = strings.Replace(result.Links.Package.Href, "https", "http", 1)

	u, err = url.Parse(result.Links.Package.Href)
	if err != nil {
		return "", "", err
	}

	req = &http.Request{
		URL:    u,
		Method: "GET",
		Header: http.Header{
			"Accept": []string{"application/json"},
		},
	}
	req = req.WithContext(ctx)

	resp, err = c.doer.Do(req)
	if err != nil {
		return "", "", err
	}

	defer func(resp *http.Response) {
		// Fail safe to ensure the clients are being cleaned up
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}(resp)

	if resp.StatusCode != http.StatusOK {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return "", "", err
		}

		return "", "", fmt.Errorf("unexpected response %d: %s", resp.StatusCode, data)
	}

	var gresult struct {
		Guid  string `json:"guid"`
		Links struct {
			Download struct {
				Href string `json:"href"`
			} `json:"download"`
		} `json:"links"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&gresult); err != nil {
		return "", "", err
	}

	if gresult.Guid == "" || gresult.Links.Download.Href == "" {
		return "", "", errors.New("empty results")
	}

	// Replace HTTPS with HTTP so the HTTP_PROXY can do the work for us
	dl := strings.Replace(gresult.Links.Download.Href, "https", "http", 1)

	return gresult.Guid, dl, nil
}

func (c *Client) GetEnvironmentVariables(ctx context.Context, appGuid string) (map[string]string, error) {
	if appGuid == "" {
		appGuid = c.appGuid
	}

	u, err := url.Parse(c.addr)
	if err != nil {
		return nil, err
	}
	u.Path = fmt.Sprintf("/v3/apps/%s/environment_variables", appGuid)

	req := &http.Request{
		URL:    u,
		Method: "GET",
		Body:   ioutil.NopCloser(bytes.NewReader(nil)),
		Header: http.Header{
			"Accept": []string{"application/json"},
		},
	}
	req = req.WithContext(ctx)

	resp, err := c.doer.Do(req)
	if err != nil {
		return nil, err
	}

	defer func(resp *http.Response) {
		// Fail safe to ensure the clients are being cleaned up
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}(resp)

	if resp.StatusCode != 200 {
		data, _ := ioutil.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, data)
	}

	var t struct {
		Var map[string]string `json:"var"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&t); err != nil {
		return nil, err
	}

	return t.Var, nil
}
