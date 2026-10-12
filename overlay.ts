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
