import { useCallback, useEffect, useRef, useState, type Dispatch, type SetStateAction } from 'react';
import type { PickedLocalFile } from '@/types';
import { useNotificationStore } from '@/store/notificationStore';
import { capturePromptDraftTarget, usePromptDraftStore } from '@/store/promptDraftStore';
import { usePromptDraftKey } from './usePromptDraftKey';
export type { PickedLocalFile } from '@/types';
export const formatFileSize = (bytes: number) => bytes < 1024 ? `${bytes} B` : bytes < 1024 * 1024 ? `${(bytes / 1024).toFixed(1)} KB` : `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
const EMPTY: PickedLocalFile[] = [];
export interface UseLocalFileReferenceParams { sessionId?: string | null; disabled: boolean; runActive: boolean; compacting: boolean; isSubmitting: boolean; isUploadingPaste: boolean }
/** Native paths remain local. A late picker response belongs only to its original draft. */
export function useLocalFileReference({sessionId, disabled, runActive, compacting, isSubmitting, isUploadingPaste}: UseLocalFileReferenceParams) {
    const draftKey = usePromptDraftKey(sessionId);
    const localFiles = usePromptDraftStore(s => s.drafts[draftKey]?.localFiles ?? EMPTY);
    const [isPickingLocalFile, setPicking] = useState(false);
    const generation = useRef(0);
    const invalidate = useCallback(() => { generation.current++; }, []);
    useEffect(() => { invalidate(); setPicking(false); return invalidate; }, [sessionId, runActive, compacting, invalidate]);
    const setLocalFiles = useCallback<Dispatch<SetStateAction<PickedLocalFile[]>>>(value => usePromptDraftStore.getState().setLocalFiles(draftKey, value), [draftKey]);
    const handlePickLocalFile = useCallback(async () => {
        if (disabled || runActive || compacting || isSubmitting || isUploadingPaste || isPickingLocalFile) return;
        const request = ++generation.current;
        const target = capturePromptDraftTarget(draftKey);
        setPicking(true);
        try {
            const response = await fetch('/api/files/pick', { method: 'POST', headers: { 'X-Zhikun-Native-Picker': '1' } });
            if (response.status === 204) return;
            if (!response.ok) throw new Error(`选择本地文件失败（HTTP ${response.status}）`);
            const data: unknown = await response.json();
            const files = (data as {files?: unknown})?.files;
            if (!Array.isArray(files) || !files.every(f => f && typeof f.path === 'string' && f.path.length > 0 && typeof f.name === 'string' && typeof f.size === 'number' && Number.isFinite(f.size) && f.size >= 0)) throw new Error('服务返回了无效的文件路径');
            const key = target();
            if (generation.current !== request || key === undefined) return;
            usePromptDraftStore.getState().setLocalFiles(key, previous => [...new Map([...previous, ...files as PickedLocalFile[]].map(f => [f.path, f])).values()]);
        } catch (error) {
            if (generation.current === request) useNotificationStore.getState().addNotification({key: 'native-file-picker', level: 'error', message: error instanceof Error ? error.message : '选择本地文件失败'});
        } finally { if (generation.current === request) setPicking(false); }
    }, [disabled, runActive, compacting, isSubmitting, isUploadingPaste, isPickingLocalFile, draftKey]);
    return {localFiles, setLocalFiles, isPickingLocalFile, fileReferenceBusy: isPickingLocalFile, fileReferenceTitle: '引用本地文件路径', handlePickLocalFile, handleFileReferenceClick: handlePickLocalFile};
}
