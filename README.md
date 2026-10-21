<h1>
  <img alt="Containerization logo" src="./icon.png" width="70" valign="middle">
  &nbsp;hub-s3
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
for (await const files: Files of service.values('s3/list', path)) {
	
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
for (await const statuses of service.values('s3/status')) {
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

