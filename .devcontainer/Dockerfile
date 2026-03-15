FROM mcr.microsoft.com/devcontainers/go:1-1.25-bookworm

# Pre-install Go tools (also auto-installed by the VS Code Go extension)
USER vscode
RUN go install golang.org/x/tools/gopls@latest \
    && go install golang.org/x/tools/cmd/goimports@latest
USER root
