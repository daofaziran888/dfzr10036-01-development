import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

// ---- 回填区间数据视图 ----

const COVERAGE_BASE = '/api/backfill-coverage'

export type CoverageSegment = {
  lo: number
  hi: number
  range: string
  state: '已覆盖' | '占用' | '待派发'
  owner: string
  length: number
}

export type CoverageOccupancy = {
  pos: number
  valid_writer: string
  executor: string
  connected: boolean
  batch_status: string
}

export type CoverageView = {
  stream: string
  gap_ranges: string[]
  covered_ranges: string[]
  occupied_ranges: string[]
  dispatchable_ranges: string[]
  segments: CoverageSegment[]
  occupancy: CoverageOccupancy[]
  counts: { gap: number; covered: number; occupied: number; dispatchable: number }
}

export type DispatchBlock = {
  lo: number
  hi: number
  range: string
  reason: string
  owner: string
  executor?: string
}

export type DispatchRecord = {
  dispatch_id: number
  batch_no: string
  stream: string
  executor: string
  request_range: string
  accepted_ranges: string[]
  blocked: DispatchBlock[]
  accepted_count: number
}

export type TargetRow = {
  stream: string
  pos: number
  writer_batch: string
  executor: string
}

async function postJson<T>(path: string, payload: Record<string, unknown>): Promise<T> {
  const response = await request(path, { method: 'POST', body: JSON.stringify(payload) })
  if (!response.ok) throw new Error('接口返回异常，操作未生效')
  return (await response.json()) as T
}

export async function fetchCoverageView(stream: string): Promise<CoverageView> {
  const response = await request(`${COVERAGE_BASE}/view/${encodeURIComponent(stream)}`)
  if (!response.ok) throw new Error('回填区间数据视图读取失败')
  return await response.json()
}

export async function fetchCoverageStreams(): Promise<string[]> {
  const response = await request(`${COVERAGE_BASE}/streams`)
  if (!response.ok) throw new Error('数据流列表读取失败')
  return (await response.json()).streams
}

export function dispatchBackfill(payload: {
  stream: string
  lo: number
  hi: number
  executor: string
  batch_no?: string
}): Promise<{ ok: boolean; message: string; dispatch: DispatchRecord; view: CoverageView }> {
  return postJson(`${COVERAGE_BASE}/dispatch`, payload)
}

export function receiveBackfillWrites(payload: {
  stream: string
  batch_no: string
  positions: number[]
  executor?: string
}): Promise<{ ok: boolean; message: string; accepted: number[]; idempotent: number[]; rejected: { pos: number; reason: string }[]; view: CoverageView }> {
  return postJson(`${COVERAGE_BASE}/writes`, payload)
}

export function interruptBackfill(payload: { stream: string; batch_no: string }) {
  return postJson<{ ok: boolean; message: string; released_ranges: string[]; view: CoverageView }>(
    `${COVERAGE_BASE}/interrupt`,
    payload,
  )
}

export function updateBatchConnection(payload: { batch_no: string; connected: boolean }) {
  return postJson<{ ok: boolean; status: string }>(`${COVERAGE_BASE}/connection`, payload)
}

export async function fetchDispatches(stream?: string): Promise<DispatchRecord[]> {
  const suffix = stream ? `?stream=${encodeURIComponent(stream)}` : ''
  const response = await request(`${COVERAGE_BASE}/dispatches${suffix}`)
  if (!response.ok) throw new Error('派发记录读取失败')
  return (await response.json()).items
}

export async function fetchTargetRows(stream: string): Promise<TargetRow[]> {
  const response = await request(`${COVERAGE_BASE}/target-rows/${encodeURIComponent(stream)}`)
  if (!response.ok) throw new Error('目标表读取失败')
  return (await response.json()).items
}

export async function resetCoverageScenario(): Promise<{ ok: boolean; message: string }> {
  const response = await request(`${COVERAGE_BASE}/reset`, { method: 'POST' })
  return await response.json()
}
