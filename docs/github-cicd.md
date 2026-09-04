# GitHub CI/CD

CI runs on pull requests and pushes to `master` using the repository-pinned Rust
toolchain (`1.85.0`). It checks formatting, strict Clippy, and the full
single-worker test suite; this includes the TCP adapter tests.

Deployment is deliberately manual. Start **Actions → Deploy**, select a
protected GitHub environment, and enter an existing tag. Configure these as
environment secrets; do not put them in workflow files:

- `DEPLOY_URL`: HTTPS base URL for the deployment receiver.
- `DEPLOY_TOKEN`: bearer token accepted by that receiver.

The receiver must accept `PUT /deploy`, the tar archive as the request body,
and the `X-Release-Tag` header. Environment approvals remain the deployment
gate; the workflow does not assume a cloud, host, SSH account, or runtime
configuration.
