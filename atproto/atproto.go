package atproto

import (
	"fmt"
	"net/url"
)

func FirehoseURL(service string, cursor int64) (string, error) {
	u, err := url.Parse(service)
	if err != nil {
		return "", fmt.Errorf("parsing service URL: %w", err)
	}
	u.Path = "xrpc/com.atproto.sync.subscribeRepos"
	if cursor > 0 {
		q := u.Query()
		q.Set("cursor", fmt.Sprintf("%d", cursor))
		u.RawQuery = q.Encode()
	}
	return u.String(), nil
}
