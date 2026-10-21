export interface S3SharePublication {
  authority: string
  protocol: 'tcp' | 'udp'
  port: number
}

const canonicalAuthority =
  /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*\.hub\.invalid$/

export function overlayS3Endpoint(publication: S3SharePublication | undefined): string {
  if (!publication) throw new Error('Hub Network S3 share is not published')
  if (publication.protocol !== 'tcp') {
    throw new Error(`Hub Network S3 share uses unsupported protocol '${publication.protocol}'`)
  }
  if (!Number.isInteger(publication.port) || publication.port < 1 || publication.port > 65_535) {
    throw new Error(`Hub Network S3 share returned invalid port '${publication.port}'`)
  }
  if (!canonicalAuthority.test(publication.authority)) {
    throw new Error(`Hub Network S3 share returned invalid authority '${publication.authority}'`)
  }
  return `http://${publication.authority}:${publication.port}`
}

/**
 * `S3_LOCAL_ENDPOINT`: an HTTP(S) origin to presign against instead of the Hub Network share, for development and
 * LANs without the overlay. Opt-in; without it URL generation stays fail-closed.
 */
export function directS3Endpoint(value: string | undefined): string | undefined {
  const endpoint = value?.trim()
  if (!endpoint) return undefined
  let url: URL
  try {
    url = new URL(endpoint)
  } catch {
    throw new Error(`Invalid S3_LOCAL_ENDPOINT '${endpoint}'`)
  }
  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    throw new Error(`S3_LOCAL_ENDPOINT uses unsupported protocol '${url.protocol}'`)
  }
  if (url.username || url.password || url.pathname !== '/' || url.search || url.hash) {
    throw new Error('S3_LOCAL_ENDPOINT must be an HTTP(S) origin without credentials, path, query, or fragment')
  }
  return url.origin
}
