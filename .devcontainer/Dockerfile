FROM mcr.microsoft.com/devcontainers/go:1-1.25-bookworm

# Install k9s
RUN ARCH="$(dpkg --print-architecture)" \
    && curl -fsSL "https://github.com/derailed/k9s/releases/latest/download/k9s_linux_${ARCH}.deb" -o /tmp/k9s.deb \
    && dpkg -i /tmp/k9s.deb \
    && rm /tmp/k9s.deb

# Pre-install Go tools (also auto-installed by the VS Code Go extension)
USER vscode
RUN go install golang.org/x/tools/gopls@latest \
    && go install golang.org/x/tools/cmd/goimports@latest
USER root
