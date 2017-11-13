## Steps to release Faktory

This takes about 30 minutes.  It's alarmingly manual, thus this
checklist.

### Prepare

- `git pull`
- Update Changes.md
- Bump version in faktory.go and Makefile.
- `make test`: verify it passes locally.

### Push Docker image

- `make dimg`: verify it builds
- `make drun`: verify it runs and localhost:7420 works
- `git ci -am "release" ; git push`: verify Travis build passes.
- `make dpush`: tags release, pushes image to DockerHub

### Update Homebrew

- `git pull`
- Download `curl -o binary https://codeload.github.com/contribsys/faktory/tar.gz/v#{version}`
- `shasum -a 256 binary`
- Edit homebrew-faktory/faktory.rb, update version and sha256
- `git ci -am "release" && git push`
- `brew upgrade faktory`
