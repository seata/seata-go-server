package prophet

import (
	"net/url"
	"strings"
)

func parseUrls(s string) ([]url.URL, error) {
	items := strings.Split(s, ",")
	urls := make([]url.URL, 0, len(items))
	for _, item := range items {
		u, err := url.Parse(item)
		if err != nil {
			return nil, err
		}

		urls = append(urls, *u)
	}

	return urls, nil
}
