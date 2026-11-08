import type { ApiSettings } from 'hub-service'

/**
 * Who may call each path. Hubs from before default-deny let anyone call a path its service doesn't guard, so every
 * path asks the hub whether its caller holds a grant of that path: owners always pass, other callers need an owner's
 * grant, anonymous callers never pass (security audit M9). A service that calls this one, like hub-s3-raid using it
 * as a node, needs a grant of the paths it calls (a `s3/` prefix grant covers them all).
 */
const granted = (path: string, name: string): ApiSettings => ({ permission: path, permissions: { name } })

export const access: Record<string, ApiSettings> = {
  's3/read': granted('s3/read', 'Read files'),
  's3/read/directory': granted('s3/read/directory', 'Read files'),
  's3/size': granted('s3/size', 'Read files'),
  's3/list': granted('s3/list', 'Read files'),
  's3/status': granted('s3/status', 'Read files'),
  's3/write': granted('s3/write', 'Upload files'),
  's3/updated': granted('s3/updated', 'Upload files'),
  's3/update/status': granted('s3/update/status', 'Upload files'),
  's3/delete': granted('s3/delete', 'Delete files'),
}
