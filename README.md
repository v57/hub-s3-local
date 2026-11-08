<h1>
  <img alt="Containerization logo" src="./icon.png" width="70" valign="middle">
  &nbsp;hub-s3-local
</h1>

> Made for [Hub](https://hub.v57.dev)

Share `~/Hub/Files` directory with your Hub network

The local S3 server selects an available port at startup and publishes that
exact TCP port to authenticated profiles through Hub Network.
Read and write requests return presigned URLs using Hub's canonical
`*.hub.invalid` publication authority and selected port. URL generation fails
closed until the share has a live Hub Network publication.

## Configuration

| Variable | Default | |
|---|---|---|
| `S3_LOCAL_FOLDER` | `~/Hub/Files` | Folder to share |
| `S3_LOCAL_PORT` | first free port from 9000 | Port of the S3 server (fails if taken) |
| `S3_LOCAL_ENDPOINT` | none | HTTP(S) origin to presign against instead of the Hub Network share, for development and LANs without the overlay (`http://127.0.0.1:9000`) |

Several instances can run on one machine with their own folder, port, working directory (the service key is
`./auth`) and `HUB_DEVICE_KEY` (a device each) or `HUB_INSTANCE` (one device).

The S3 server's access keys are random on every start and never leave the process: callers get presigned links only.
The server listens on every interface (the default of the [s3](https://github.com/v57/s3) package), so a link also
works from the LAN when the machine is reachable there.

## Installing on a Hub

hub-s3-local is in the launcher's app catalogue as **Files** (`v57/hub-s3-local`). The Hub app installs it from
**Files** → **Connect Storage** → **Local** (it creates the launcher app **Local Storage**; see the Hub app's
[Files guide](https://github.com/v57/hub-app/blob/main/docs/Files.md)), or from **Launcher** → **Get More**. The `hub`
CLI's client installs it from its store page (`hub client launcher/store`). It also runs by hand next to a hub:

```sh
bun install
HUB=ws://127.0.0.1:1997 S3_LOCAL_FOLDER=~/Shared bun start
```

Besides the variables above it reads `HUB` (hub address, `1997` by default) and the SDK's `HUB_DEVICE_KEY` and
`HUB_INSTANCE`, which the launcher sets for its apps. It needs no secrets. The service key is the `auth` file in the
working directory, created on the first run. The hub doesn't route `s3/*` to it until an owner allows it on the Hub
app's **Requests** page (the Connect Storage page offers **Allow** for it). Links work once the Hub Network share `s3`
is published, or right away with `S3_LOCAL_ENDPOINT`.

## Download file

```ts
const url: string = await service.send('s3/read', 'folder/some.txt')
const data = await fetch(url)
```

## Upload file

```ts
const url: string = await service.send('s3/write', 'folder/some.txt')
await fetch(url, { method: 'PUT', body: Bun.file('some.txt') })
// Send completion request to notify everyone who is listening to this folder
await service.send('s3/updated', 'folder/')
// You use same url to download it
const file = await fetch(url)
```

## Get file size

```ts
const size: number = await service.send('s3/size', 'folder/some.txt')
```

## Delete file

```ts
await service.send('s3/delete', 'folder/some.txt')
```

### Delete folder

```ts
await service.send('s3/delete', 'folder/')
```

## Realtime updates

```ts
const path: string | undefined
for await (const files of service.values('s3/list', path)) {
  // Files
	
}
```

## Upload status

```ts
// Set status
const status: FileStatus = { name: 'Uploading', progress: 0.5 }
await service.send('s3/update/status', { path: 'some.txt', status })
// Clear status
await service.send('s3/update/status', { path: 'some.txt' })
```

## Status stream

```ts
for await (const statuses of service.values('s3/status')) {
  // Record<string, FileStatus | undefined>
}
```

## Types

```ts
interface Files {
  count: number
  files: FileInfo[]
  directories: string[]
}
interface FileInfo {
  name: string
  size: number
  lastModified?: string
}
interface FileStatus {
  name: 'Uploading' | 'Processing'
  progress: number
}
```

## API

The same paths as [hub-s3](https://github.com/v57/hub-s3), each described for `hub/api/list` in `descriptions.ts`.
Links are presigned URLs to the local S3 server, valid for 7 days: anyone who holds one and reaches the server (through
the Hub Network share, or the `S3_LOCAL_ENDPOINT` origin) can download, or upload and overwrite, that file.

| Path | Kind | Title | Takes | Returns |
|---|---|---|---|---|
| `s3/read` | post | Download link | file path | presigned GET URL; fails with `Hub Network S3 share is not published` until it is |
| `s3/write` | post | Upload link | file path | presigned PUT URL (same failure); call `s3/updated` after the upload |
| `s3/size` | post | File size | file path | bytes; fails when the file doesn't exist |
| `s3/list` | post | List a folder | folder ending in `/`, or nothing for the top level | `Files`: one level, at most 1000 entries |
| `s3/list` | stream | Watch a folder | same | `Files`, again after `s3/delete` or `s3/updated` changes it |
| `s3/read/directory` | post | List files recursively | path prefix, or nothing for the whole folder | `FileInfo[]` with full paths, at most 1000 |
| `s3/delete` | post | Delete a file or folder | file path, or folder ending in `/` | nothing; a folder deletes up to 1000 files per call |
| `s3/updated` | post | Refresh folder listings | folder, or nothing for every stream | nothing |
| `s3/update/status` | post | Set upload status | `{ path, status? }` | nothing; no `status` clears it, kept in memory |
| `s3/status` | stream | Watch upload status | nothing | `Record<path, FileStatus>`, again after every change |

## Permissions

Every path asks the hub (`hub/grant/check`) whether the caller holds a grant of that same path, so only owners and
callers an owner granted the path may call it, in `default-deny` and `default-allow` hubs alike; anonymous callers
never pass. The permissions show in the Hub app's **Permissions** table in the `s3` group:

| Permission | Paths |
|---|---|
| Read files | `s3/read`, `s3/read/directory`, `s3/size`, `s3/list`, `s3/status` |
| Upload files | `s3/write`, `s3/updated`, `s3/update/status` |
| Delete files | `s3/delete` |

An owner grants paths one by one, or all of them with a `prefix` grant on `s3/`:

```ts
await owner.send('hub/grant/add', { subject: { group: 'family' }, object: { prefix: 's3/' } })
await owner.send('hub/grant/add', { subject: { device: '<device id>' }, object: { path: 's3/read' } })
```

A service that uses this one as storage, like hub-s3-raid, needs such a grant for its own key. Reaching the share
itself goes through the Hub Network's own rules for authenticated profiles.

## Testing

```sh
bun install
bun test
```

`overlay.test.ts` covers how links are built from the share's publication (and fail closed without one) and
`S3_LOCAL_ENDPOINT`'s checks. `access.test.ts` checks every registered path is guarded and that anonymous and
ungranted callers are refused while owners and granted callers pass (the hub's grant answers are stubbed).
`descriptions.test.ts` checks every registered path is described with schemas in the subset.

`bun run test:linux-overlay` (`integration/two_agent_overlay.sh`) is a Linux acceptance test: as root, with `ip`,
`nft`, `wg`, `curl` and `python3`, it puts a caller and a provider in network namespaces joined by real WireGuard
interfaces, serves S3 on the provider and uploads and downloads through `*.hub.invalid` presigned links. It skips
itself elsewhere.
