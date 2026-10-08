/**
 * usePromptAttachments — PromptInput 图片附件逻辑
 *
 * §8.3.1 PromptInput 拆分：从原 PromptInput.tsx 纯搬运（零行为变化）。
 * 职责：图片附件的按钮 / 拖拽 / 粘贴上传（本地 Base64 消息附件），
 * 附件移除与异步链路卸载时的 ObjectURL 回收防护。
 *
 * 草稿持久化（P1 修复）：附件列表按活动 sessionId 键控托管到
 * promptDraftStore（仅内存，理由同 usePromptState 的输入草稿），
 * 卸载/重挂载后附件可恢复，不同会话相互隔离。
 */

import {
    useCallback,
    useEffect,
    useMemo,
    useRef,
    useState,
    type ClipboardEvent,
    type Dispatch,
    type SetStateAction,
} from 'react';
import type {
    LocalAttachment,
} from '@/types';
import { useNotificationStore } from '@/store/notificationStore';
import {
    capturePromptDraftTarget,
    usePromptDraftStore,
} from '@/store/promptDraftStore';
import { useModelStore } from '@/store/modelStore';
import { useSessionStore } from '@/store/sessionStore';
import { generateUUID } from '@/utils/uuid';
import { usePromptDraftKey } from './usePromptDraftKey';

/** 单张图片附件大小上限：5MB */
const MAX_IMAGE_SIZE = 5 * 1024 * 1024;

/**
 * 将 File 读取为纯 base64 字符串（去除 data:mime;base64, 前缀）。
 * 后端 Attachment.base64Data 期望接收不含前缀的 base64。
 */
function readFileAsBase64(file: File): Promise<string> {
    return new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onload = () => {
            const result = reader.result as string;
            const commaIdx = result.indexOf(',');
            resolve(commaIdx >= 0 ? result.slice(commaIdx + 1) : result);
        };
        reader.onerror = () => reject(reader.error ?? new Error('FileReader failed'));
        reader.readAsDataURL(file);
    });
}

export interface UsePromptAttachmentsParams {
    runActive: boolean;
    compacting: boolean;
    /** 草稿归属会话：附件随 sessionId 键控存入 promptDraftStore；缺省回落稳定兜底键 */
    sessionId?: string | null;
}

/** 选择器兜底空数组（模块级常量保证引用稳定，避免无意义重渲染） */
const EMPTY_ATTACHMENTS: LocalAttachment[] = [];

export function usePromptAttachments({
    runActive,
    compacting,
    sessionId,
}: UsePromptAttachmentsParams) {
    // 附件托管到 promptDraftStore（按活动 sessionId 键控，仅内存）：
    // 卸载/重挂载（移动端底部导航切换）后从 store 读回，ObjectURL 保持有效。
    const draftKey = usePromptDraftKey(sessionId);
    const attachments = usePromptDraftStore(
        s => s.drafts[draftKey]?.attachments ?? EMPTY_ATTACHMENTS,
    );
    const setAttachments = useCallback<Dispatch<SetStateAction<LocalAttachment[]>>>((value) => {
        usePromptDraftStore.getState().setAttachments(draftKey, value);
    }, [draftKey]);
    const [isUploadingPaste, setIsUploadingPaste] = useState(false);
    // 异步链路（粘贴上传/读取 base64）的卸载防护：卸载后短路 setState/通知并回收 ObjectURL
    const isMountedRef = useRef(true);

    useEffect(() => {
        isMountedRef.current = true;
        return () => {
            isMountedRef.current = false;
        };
    }, []);

    const selectedModel = useSessionStore(state => state.model);
    const models = useModelStore(state => state.models);
    const defaultModel = useModelStore(state => state.defaultModel);
    const loading = useModelStore(state => state.loading);
    const loaded = useModelStore(state => state.loaded);
    const error = useModelStore(state => state.error);
    const modelInfo = models.find(model => model.id === (selectedModel ?? defaultModel));
    const limit = modelInfo?.maxImages;
    const knownLimit = typeof limit === 'number' && Number.isSafeInteger(limit) && limit >= 0;
    const imageCapability: 'loading' | 'unavailable' | 'unsupported' | 'ready' = loading || (!loaded && !error && models.length === 0)
        ? 'loading' : error || !knownLimit ? 'unavailable' : limit === 0 ? 'unsupported' : 'ready';
    const maxImages = knownLimit ? limit : 0;
    const imageCapabilityMessage = imageCapability === 'loading' ? '图片能力正在加载，请稍后添加图片'
        : imageCapability === 'unavailable' ? '图片能力暂不可用，请重试加载模型目录'
        : imageCapability === 'unsupported' ? '当前模型没有可用的图片处理能力'
        : `上传图片（当前模型有效上限 ${maxImages} 张）`;
    const retryImageCapabilities = useCallback(() => useModelStore.getState().fetchModels(), []);
    const notifyUnavailable = useCallback(() => {
        useNotificationStore.getState().addNotification({ key: 'image-capability-unavailable', level: 'warning',
            message: imageCapabilityMessage, ...(imageCapability === 'unavailable' ? { onRetry: retryImageCapabilities } : {}) });
    }, [imageCapability, imageCapabilityMessage, retryImageCapabilities]);

    const imageCount = useMemo(
        () => attachments.filter(a => a.type.startsWith('image/')).length,
        [attachments]
    );

    const handleFiles = useCallback(async (
        files: File[],
        resolveTarget = capturePromptDraftTarget(draftKey),
    ) => {
        if (runActive || compacting) {
            useNotificationStore.getState().addNotification({
                key: 'run-input-attachments',
                level: 'warning',
                message: '任务运行或压缩期间不能添加附件',
                timeout: 5000,
            });
            return;
        }
        if (imageCapability !== 'ready') { notifyUnavailable(); return; }
        const accepted: LocalAttachment[] = [];
        const notify = useNotificationStore.getState().addNotification;
        // 使用独立计数器避免同一批多张图片同时越限
        let currentImageCount = imageCount;

        // 仅接受图片类型文件；非图片文件统一过滤并提示一次
        const nonImages = files.filter(f => !f.type.startsWith('image/'));
        const imageFiles = files.filter(f => f.type.startsWith('image/'));
        if (nonImages.length > 0) {
            notify({
                key: `attach-nonimage-ignored-${generateUUID()}`,
                level: 'warning',
                message: `仅支持上传图片文件，已忽略 ${nonImages.length} 个非图片文件`,
            });
        }

        for (const f of imageFiles) {
            // 组件卸载后停止处理后续图片，避免卸载后继续读文件/发通知
            if (!isMountedRef.current) break;
            const isImage = f.type.startsWith('image/');

            // 超出通用图片数量上限：静默丢弃剩余图片，仅提示一次
            if (isImage && currentImageCount >= maxImages) {
                notify({
                    key: `attach-img-limit-${generateUUID()}`,
                    level: 'warning',
                    message: `已达图片数量上限 (${currentImageCount}/${maxImages})`,
                });
                continue;
            }

            // 图片单独校验大小上限
            if (isImage && f.size > MAX_IMAGE_SIZE) {
                notify({
                    key: `attach-too-large-${generateUUID()}`,
                    level: 'warning',
                    message: `图片 “${f.name}” 超出 5MB 上限，已跳过`,
                });
                continue;
            }

            const base: LocalAttachment = {
                id: generateUUID(),
                name: f.name,
                size: f.size,
                type: f.type,
                file: f,
            };

            if (isImage) {
                try {
                    base.base64Content = await readFileAsBase64(f);
                    base.previewUrl = URL.createObjectURL(f);
                    currentImageCount += 1;
                } catch (err) {
                    if (!isMountedRef.current) break;
                    notify({
                        key: `attach-read-fail-${generateUUID()}`,
                        level: 'error',
                        message: `读取图片 “${f.name}” 失败：${(err as Error).message}`,
                    });
                    continue;
                }
            }

            accepted.push(base);
        }

        // 卸载后不再 setState；已创建的预览 URL 未进入 attachmentsRef，需就地回收
        const targetKey = resolveTarget();
        if (!isMountedRef.current || targetKey === undefined) {
            accepted.forEach(a => {
                if (a.previewUrl) URL.revokeObjectURL(a.previewUrl);
            });
            return;
        }

        if (accepted.length > 0) {
            usePromptDraftStore.getState().setAttachments(targetKey, prev => {
                const remaining = Math.max(0, maxImages - prev.filter(a => a.type.startsWith('image/')).length);
                accepted.slice(remaining).forEach(a => { if (a.previewUrl) URL.revokeObjectURL(a.previewUrl); });
                return [...prev, ...accepted.slice(0, remaining)];
            });
        }
    }, [imageCount, maxImages, runActive, compacting, draftKey, imageCapability, notifyUnavailable]);

    const handlePaste = useCallback((event: ClipboardEvent<HTMLTextAreaElement>) => {
        const itemFiles = Array.from(event.clipboardData.items)
            .filter(item => item.kind === 'file' && item.type.startsWith('image/'))
            .map(item => item.getAsFile())
            .filter((file): file is File => file !== null);
        const imageFiles = itemFiles.length > 0
            ? itemFiles
            : Array.from(event.clipboardData.files).filter(file => file.type.startsWith('image/'));
        if (imageFiles.length === 0) return;
        event.preventDefault();

        if (runActive || compacting || isUploadingPaste) {
            useNotificationStore.getState().addNotification({
                key: `paste-image-busy-${generateUUID()}`,
                level: 'warning',
                message: '当前任务运行、压缩或图片上传期间不能粘贴图片',
            });
            return;
        }

        if (imageCapability !== 'ready') { notifyUnavailable(); return; }
        const remaining = Math.max(0, maxImages - imageCount);
        const accepted = imageFiles.slice(0, remaining).filter(file => file.size <= MAX_IMAGE_SIZE);
        const notify = useNotificationStore.getState().addNotification;
        if (imageFiles.some(file => file.size > MAX_IMAGE_SIZE)) {
            notify({
                key: `paste-image-size-${generateUUID()}`,
                level: 'warning',
                message: '部分粘贴图片超过 5MB，已跳过',
            });
        }
        if (imageFiles.length > remaining) {
            notify({
                key: `paste-image-limit-${generateUUID()}`,
                level: 'warning',
                message: `图片数量最多为 ${maxImages} 张，超出部分已跳过`,
            });
        }
        if (accepted.length === 0) return;

        setIsUploadingPaste(true);
        const resolveTarget = capturePromptDraftTarget(draftKey);
        void handleFiles(accepted, resolveTarget).catch(error => {
            if (!isMountedRef.current) return;
            notify({
                key: `paste-image-failed-${generateUUID()}`,
                level: 'error',
                message: error instanceof Error ? error.message : '读取粘贴图片失败',
                timeout: 7000,
            });
        }).finally(() => {
            if (isMountedRef.current) setIsUploadingPaste(false);
        });
    }, [compacting, handleFiles, imageCount, isUploadingPaste, maxImages,
        runActive, draftKey, imageCapability, notifyUnavailable]);

    // Drag & drop file upload
    const handleDrop = useCallback((e: React.DragEvent) => {
        e.preventDefault();
        const dropped = Array.from(e.dataTransfer.files);
        // 仅接受图片文件；非图片文件直接过滤掉
        const imagesOnly = dropped.filter(f => f.type.startsWith('image/'));
        const nonImageCount = dropped.length - imagesOnly.length;
        if (nonImageCount > 0) {
            useNotificationStore.getState().addNotification({
                key: `drop-nonimage-ignored-${generateUUID()}`,
                level: 'warning',
                message: '非图片文件不会上传，请使用“引用本地文件路径”按钮选择',
            });
        }
        if (imagesOnly.length === 0) return;
        void handleFiles(imagesOnly);
    }, [handleFiles]);

    const removeAttachment = useCallback((id: string) => {
        setAttachments(prev => {
            const target = prev.find(a => a.id === id);
            if (target?.previewUrl) {
                URL.revokeObjectURL(target.previewUrl);
            }
            return prev.filter(a => a.id !== id);
        });
    }, [setAttachments]);

    // 注意：附件随草稿在组件卸载后继续存活于 promptDraftStore（重挂载需原样恢复），
    // 因此不再于卸载时回收预览 ObjectURL。回收点收敛为：removeAttachment 移除、
    // usePromptState.handleSubmit 提交成功、以及上述异步链路的卸载防护（isMountedRef）。

    // 注：模型切换不再清理已选图片附件。
    // 后端的智能视觉路由会在请求时自动选择同厂商视觉模型处理图片，
    // 因此即便切换到 supportsImages=false 的模型也无需移除图片。

    return {
        attachments,
        setAttachments,
        isUploadingPaste,
        imageCount,
        maxImages,
        imageCapability,
        imageCapabilityMessage,
        retryImageCapabilities,
        notifyUnavailable,
        handleFiles,
        handlePaste,
        handleDrop,
        removeAttachment,
    };
}
