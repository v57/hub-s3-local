import { S3Client } from 'bun'
import { S3Server } from 's3'

const mode = process.argv[2]
const accessKeyId = 'hn502-access-key'
const secretAccessKey = 'hn502-secret-key'
const bucket = 'hub'
const port = 9001

if (mode === 'serve') {
  const directory = process.argv[3]
  if (!directory) throw new Error('serve requires storage directory')
  await new S3Server({
    key: accessKeyId,
    secret: secretAccessKey,
    bucketPaths: { [bucket]: directory },
    port,
  }).start()
  await new Promise(() => {})
}

if (mode === 'presign') {
  const authority = process.argv[3]
  const object = process.argv[4]
  if (!authority || !object) throw new Error('presign requires authority and object')
  const client = new S3Client({
    accessKeyId,
    secretAccessKey,
    bucket,
    endpoint: `http://${authority}:${port}`,
  })
  console.log(client.presign(object, { acl: 'public-read-write', method: 'PUT', expiresIn: 60 }))
  console.log(client.presign(object, { acl: 'public-read', method: 'GET', expiresIn: 60 }))
}
