Steps to release Faktory

- Update Changes.md
- Bump version in faktory.go and Makefile.
- `make test`: verify it passes locally.
- `make dimg`: verify it builds
- `make drun`: verify it runs and localhost:7420 works
- `git ci -am "release" ; git push`: verify Travis build passes.
- `make dpush`: tags release, pushes image to DockerHub

