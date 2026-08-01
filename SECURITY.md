# Security policy

Please report suspected vulnerabilities privately to the repository maintainers rather than opening a public issue. Include the affected version, reproduction steps, and impact where possible.

Mirror operators should use a bucket-scoped service account, Application Default Credentials, object-versioning or retention controls appropriate to their deployment, and a dedicated local state directory. Configuration files must never contain credentials.

