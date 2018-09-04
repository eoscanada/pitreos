llerrgroup
==========

`llerrgroup` augments `golang.org/x/sync/errgroup` to add parallelism
to the `errgroup`.

If an error occurs, the iteration will be short-circuited and the
first error returned.

Sample usage:

```go
    urls := []string{"http://www.google.com", "http://www.bing.com", "add more..."}
	eg := llerrgroup.New(3)  // 3 parallel goroutines
	for _, url := range urls {
		if eg.Stop() {
			continue  // short-circuit the loop if we got an error
		}

		url := url  // lock value in loop's scope. The `func() error` below is the required signature to `Go()`.
		eg.Go(func() error {
			_, err := http.Get(url)
			return err
		})
	}
	if err := eg.Wait(); err != nil {
		// eg.Wait() will block until everything is done, and return the first error.
		return err
	}

```


License
-------

MIT licensed.
