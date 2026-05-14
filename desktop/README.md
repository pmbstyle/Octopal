# Octopal Desktop

Electron desktop installer and control center for Octopal.

## Development

```bash
npm install
npm run dev
```

## Build

```bash
npm run build
```

The build command type-checks the Electron app and writes compiled assets to `out/`.

## Local Packaging

```bash
npm run pack
npm run dist:win
npm run dist:mac
npm run dist:linux
```

Packaged artifacts are written to `dist/`. Platform installers are built with `electron-builder`:

- Windows: NSIS installer
- macOS: DMG
- Linux: AppImage

## Release

The repository release workflow builds desktop artifacts on Windows, macOS, and Linux for release tags, sets the desktop package version from the release tag, then uploads the installers to the GitHub release.
