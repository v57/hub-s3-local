/**
 * What a path says about itself in `hub/api/list` (protocol.md section 15): passed in the settings of `post` and
 * `stream`, next to `access`. hub-service before descriptions ignores these fields.
 */
export interface Described {
  title?: string
  description: string
  inputSchema?: Record<string, unknown>
  outputSchema?: Record<string, unknown>
  annotations?: { readOnlyHint?: boolean; destructiveHint?: boolean; idempotentHint?: boolean; openWorldHint?: boolean }
}
// Typed, not `satisfies`: schema literals would widen to `type: string`, which ApiSettings' JSONSchema doesn't take
export type Descriptions = { post: Record<string, Described>; stream: Record<string, Described> }

const filePath = { type: 'string', description: 'File path in the shared folder, like "photos/cat.jpg"' }
const folder = {
  type: 'string',
  description: 'Folder, ending in "/" ("photos/"); empty or absent for the top level',
}
const link = {
  type: 'string',
  format: 'uri',
  description: 'Presigned URL of the local S3 server through its Hub Network share (or S3_LOCAL_ENDPOINT), valid for 7 days',
}
const fileInfo = {
  type: 'object',
  properties: {
    name: { type: 'string', description: 'File name' },
    size: { type: 'integer', minimum: 0, description: 'Bytes' },
    lastModified: { type: 'string', format: 'date-time', description: 'When the file last changed' },
  },
  required: ['name', 'size'],
}
const files = {
  type: 'object',
  description: 'One folder level',
  properties: {
    count: { type: 'integer', minimum: 0, description: 'Files plus subfolders, as S3 counts them (KeyCount)' },
    files: { type: 'array', items: fileInfo, description: 'Files directly in the folder; names relative to it' },
    directories: {
      type: 'array',
      items: { type: 'string' },
      description: 'Subfolders, relative to the folder and ending in "/" ("2024/")',
    },
  },
  required: ['count', 'files', 'directories'],
}
const fileStatus = {
  type: 'object',
  description: 'What is happening to a file',
  properties: {
    name: { type: 'string', enum: ['Uploading', 'Processing'] },
    progress: { type: 'number', description: 'From 0 to 1' },
  },
  required: ['name', 'progress'],
}

export const descriptions: Descriptions = {
  post: {
    's3/read': {
      title: 'Download link',
      description:
        'A presigned GET URL for one file of the shared folder (~/Hub/Files by default), valid for 7 days; anyone who holds it and reaches the Hub Network share can download the file. It does not check that the file exists, and fails with "Hub Network S3 share is not published" until the share is live. Owners and callers granted s3/read may call it.',
      inputSchema: filePath,
      outputSchema: link,
      annotations: { readOnlyHint: true },
    },
    's3/read/directory': {
      title: 'List files recursively',
      description:
        'Every file under a prefix, at every level, with full paths; an empty or absent prefix lists the whole shared folder. One S3 listing: at most 1000 files, no paging. Owners and callers granted s3/read/directory may call it.',
      inputSchema: { type: 'string', description: 'Path prefix, usually a folder ending in "/" ("photos/"); empty or absent for the whole shared folder' },
      outputSchema: {
        type: 'array',
        items: { ...fileInfo, properties: { ...fileInfo.properties, name: { type: 'string', description: 'Full path' } } },
      },
      annotations: { readOnlyHint: true },
    },
    's3/write': {
      title: 'Upload link',
      description:
        'A presigned PUT URL for one file of the shared folder, valid for 7 days: PUT the bytes to it, then call s3/updated with the folder so s3/list streams show the file. Anyone who holds it and reaches the share can upload or overwrite that file; fails with "Hub Network S3 share is not published" until the share is live. Owners and callers granted s3/write may call it.',
      inputSchema: filePath,
      outputSchema: link,
    },
    's3/delete': {
      title: 'Delete a file or folder',
      description:
        'Deletes one file, or every file under a path ending in "/" (from one listing: at most 1000 files per call), and refreshes the s3/list streams. Answers nothing. Owners and callers granted s3/delete may call it.',
      inputSchema: { type: 'string', description: 'File path ("photos/cat.jpg"), or a folder ending in "/" ("photos/")' },
      annotations: { destructiveHint: true, idempotentHint: true },
    },
    's3/size': {
      title: 'File size',
      description: 'Size of one file in bytes. Fails when the file does not exist. Owners and callers granted s3/size may call it.',
      inputSchema: filePath,
      outputSchema: { type: 'integer', minimum: 0, description: 'Bytes' },
      annotations: { readOnlyHint: true },
    },
    's3/list': {
      title: 'List a folder',
      description:
        'One folder level: the files directly in a folder and its subfolders, from one S3 listing (at most 1000 entries). The s3/list stream follows the same listing. Owners and callers granted s3/list may call it.',
      inputSchema: folder,
      outputSchema: files,
      annotations: { readOnlyHint: true },
    },
    's3/updated': {
      title: 'Refresh folder listings',
      description:
        'Tells the s3/list streams of a folder to list it again; call it after uploading through an s3/write URL. Pass the folder exactly as the streams follow it ("photos/"), or nothing to refresh every stream. Owners and callers granted s3/updated may call it.',
      inputSchema: { type: 'string', description: 'Folder ending in "/" ("photos/"); empty or absent refreshes every s3/list stream' },
      annotations: { idempotentHint: true },
    },
    's3/update/status': {
      title: 'Set upload status',
      description:
        'Sets or clears the status of one file in the s3/status stream, like upload progress; leave out status to clear it. Kept in memory until the service restarts. Owners and callers granted s3/update/status may call it.',
      inputSchema: {
        type: 'object',
        properties: { path: filePath, status: { ...fileStatus, description: 'New status; absent clears it' } },
        required: ['path'],
      },
      annotations: { idempotentHint: true },
    },
  },
  stream: {
    's3/list': {
      title: 'Watch a folder',
      description:
        'One folder level, like the s3/list post, sent again when s3/delete or s3/updated changes it; uploads through s3/write URLs show up only after someone calls s3/updated. Owners and callers granted s3/list may call it.',
      inputSchema: folder,
      outputSchema: files,
      annotations: { readOnlyHint: true },
    },
    's3/status': {
      title: 'Watch upload status',
      description:
        'The status of every file that has one, as a map from file path to status, sent again on every s3/update/status. Owners and callers granted s3/status may call it.',
      outputSchema: { type: 'object', description: 'File path → status', additionalProperties: fileStatus },
      annotations: { readOnlyHint: true },
    },
  },
}
