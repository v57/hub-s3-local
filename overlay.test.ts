import { describe, expect, test } from 'bun:test'
import { S3Client } from 'bun'
import { overlayS3Endpoint } from './overlay'

describe('overlay S3 endpoint', () => {
  test('uses canonical publication authority and runtime-selected port', () => {
    const endpoint = overlayS3Endpoint({
      authority: 'files-a1b2c3.v1-0123456789abcdef.hub.invalid',
      protocol: 'tcp',
      port: 9001,
    })
    expect(endpoint).toBe('http://files-a1b2c3.v1-0123456789abcdef.hub.invalid:9001')

    const client = new S3Client({
      accessKeyId: 'test-access-key',
      secretAccessKey: 'test-secret-key',
      bucket: 'hub',
      endpoint,
    })
    const url = new URL(client.presign('folder/some file.txt', { method: 'GET', expiresIn: 60 }))

    expect(url.hostname).toBe('files-a1b2c3.v1-0123456789abcdef.hub.invalid')
    expect(url.port).toBe('9001')
    expect(url.pathname).toBe('/hub/folder/some%20file.txt')
    expect(url.searchParams.get('X-Amz-SignedHeaders')).toBe('host')
  })

  test('fails closed until Hub publishes the share', () => {
    expect(() => overlayS3Endpoint(undefined)).toThrow('Hub Network S3 share is not published')
  })

  test('rejects non-canonical or unusable publication results', () => {
    expect(() => overlayS3Endpoint({
      authority: 'public.example.com',
      protocol: 'tcp',
      port: 9001,
    })).toThrow('invalid authority')
    expect(() => overlayS3Endpoint({
      authority: 'files.v1-hub.hub.invalid',
      protocol: 'udp',
      port: 9001,
    })).toThrow("unsupported protocol 'udp'")
    expect(() => overlayS3Endpoint({
      authority: 'files.v1-hub.hub.invalid',
      protocol: 'tcp',
      port: 0,
    })).toThrow("invalid port '0'")
  })
})
