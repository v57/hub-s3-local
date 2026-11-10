/**
 * Presigned URL options for s3/read and s3/write. No `acl`: the object keeps the bucket's default (private), so only
 * holders of a presigned URL reach it. These used `public-read` and `public-read-write`, which on buckets that honour
 * ACLs made every upload readable by anyone without a link (security audit M11).
 */
export const read: Bun.S3FilePresignOptions = { method: 'GET', expiresIn: 86400 * 7 }
export const write: Bun.S3FilePresignOptions = { method: 'PUT', expiresIn: 86400 * 7 }
