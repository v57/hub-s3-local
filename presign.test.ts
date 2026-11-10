import { describe, expect, test } from 'bun:test'
import { S3Client } from 'bun'
import { read, write } from './presign'

describe('presign', () => {
  const client = new S3Client({ accessKeyId: 'test', secretAccessKey: 'test', bucket: 'files', endpoint: 'http://127.0.0.1:9' })

  test('read and write URLs carry no ACL, so uploads stay private', () => {
    for (const options of [read, write]) {
      expect(options.acl).toBeUndefined()
      const url = new URL(client.presign('folder/file.txt', options))
      expect([...url.searchParams.keys()].map(key => key.toLowerCase())).not.toContain('x-amz-acl')
    }
  })

  test('read is a GET and write a PUT', () => {
    expect(read.method).toBe('GET')
    expect(write.method).toBe('PUT')
  })
})
