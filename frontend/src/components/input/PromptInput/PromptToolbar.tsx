/**
 * PromptToolbar — 附件预览条（PromptAttachmentBar）+ 附件/语音操作行（PromptToolbar）
 *
 * §8.3.1 PromptInput 拆分：从原 PromptInput.tsx 纯搬运（零行为变化）。
 * 预览条与操作行均为附件相关 UI，内聚于同一模块；
 * 预览条渲染在输入行上方，操作行渲染在输入行内（与原始 DOM 结构一致）。
 */

import React from 'react';
import { FileSymlink, Loader2, Paperclip, X } from 'lucide-react';
import type {
    LocalAttachment,
} from '@/types';
import FileUpload from '../FileUpload';
import VoiceInputButton from '../VoiceInputButton';
import { formatFileSize, type PickedLocalFile } from './useLocalFileReference';

interface PromptAttachmentBarProps {
    attachments: LocalAttachment[];
    imageCount: number;
    maxImages: number;
    localFiles: PickedLocalFile[];
    onRemoveAttachment: (id: string) => void;
    setLocalFiles: React.Dispatch<React.SetStateAction<PickedLocalFile[]>>;
}

export const PromptAttachmentBar: React.FC<PromptAttachmentBarProps> = ({
    attachments,
    imageCount,
    maxImages,
    localFiles,
    onRemoveAttachment,
    setLocalFiles,
}) => (
    <>
        {/* Attachment preview bar */}
        {attachments.length > 0 && (
            <div className="mb-2">
                {/* 图片计数 badge：仅在能力已加载且存在图片附件时展示 */}
                {imageCount > 0 && maxImages > 0 && (
                    <div className="flex items-center gap-1 mb-1.5">
                        <span
                            className={`text-[13px] px-1.5 py-0.5 rounded-sm border
                                ${imageCount >= maxImages
                                    ? 'text-warnstrong border-warn bg-warnsoft'
                                    : 'text-t3 border-hairline bg-sunken2'}`}
                            title={imageCount >= maxImages ? '已达当前模型图片上限' : undefined}
                        >
                            {imageCount}/{maxImages} 张图片
                        </span>
                    </div>
                )}
                <div className="flex gap-2 flex-wrap">
                {attachments.map(a => (
                    a.previewUrl ? (
                        // 图片缩略图预览 (60x60)
                        <div
                            key={a.id}
                            className="relative group rounded-sm border border-hairline overflow-hidden
                                       bg-surfacev2"
                            style={{ width: 60, height: 60 }}
                            title={`${a.name} (${formatFileSize(a.size)})`}
                        >
                            <img
                                src={a.previewUrl}
                                alt={a.name}
                                className="w-full h-full object-cover"
                            />
                            <button
                                onClick={() => onRemoveAttachment(a.id)}
                                type="button"
                                aria-label={`移除 ${a.name}`}
                                className="panel-control absolute top-0.5 right-0.5 p-0.5 rounded-full
                                           bg-overlay2 text-white hover:bg-overlay2 hover:text-white
                                           opacity-80 group-hover:opacity-100 transition-opacity"
                            >
                                <X size={12} />
                            </button>
                        </div>
                    ) : (
                        <span
                            key={a.id}
                            className="flex items-center gap-1 px-2 py-1 bg-surfacev2 rounded-sm text-[13px] text-t2
                                       border border-hairline"
                        >
                            📎 {a.name}
                            <span className="text-t3">
                                ({formatFileSize(a.size)})
                            </span>
                            <button
                                onClick={() => onRemoveAttachment(a.id)}
                                className="panel-control ml-1 text-t3 hover:text-t1"
                                type="button"
                                aria-label={`移除 ${a.name}`}
                            >
                                ×
                            </button>
                        </span>
                    )
                ))}
                </div>
            </div>
        )}

        {localFiles.length > 0 && (
            <div className="mb-2 flex gap-2 flex-wrap">
                {localFiles.map(file => (
                    <span
                        key={file.path}
                        title={file.path}
                        className="flex max-w-full items-center gap-1 rounded-sm border border-warn bg-warnsoft px-2 py-1 text-[13px] text-warnstrong"
                    >
                        <FileSymlink size={13} className="shrink-0" />
                        <span className="truncate">{file.name}</span>
                        <span className="shrink-0 text-warn">({formatFileSize(file.size)})</span>
                        <button
                            type="button"
                            aria-label={`移除本地路径 ${file.name}`}
                            onClick={() => setLocalFiles(previous =>
                                previous.filter(item => item.path !== file.path))}
                            className="panel-control ml-1 shrink-0 text-warn hover:text-warnstrong"
                        >
                            <X size={12} />
                        </button>
                    </span>
                ))}
                <span className="self-center text-[13px] text-t3">
                    路径会发送给模型服务商；读取项目外文件仍需授权
                </span>
            </div>
        )}


    </>
);

interface PromptToolbarProps {
    runActive: boolean;
    compacting: boolean;
    disabled: boolean;
    isSubmitting: boolean;
    isUploadingPaste: boolean;
    fileReferenceBusy: boolean;
    fileReferenceTitle: string;
    asrAvailable: boolean;
    maxImages: number;
    imageCapability: 'loading' | 'unavailable' | 'unsupported' | 'ready';
    imageCapabilityMessage: string;
    onRetryImages: () => void;
    onFileReferenceClick: () => void;
    onFiles: (files: File[]) => void;
    onVoiceTranscript: (text: string) => void;
}

export const PromptToolbar: React.FC<PromptToolbarProps> = ({
    runActive,
    compacting,
    disabled,
    isSubmitting,
    isUploadingPaste,
    fileReferenceBusy,
    fileReferenceTitle,
    asrAvailable,
    maxImages,
    imageCapability,
    imageCapabilityMessage,
    onRetryImages,
    onFileReferenceClick,
    onFiles,
    onVoiceTranscript,
}) => (
    <>
        {!runActive && !compacting && (
            <button
                type="button"
                onClick={onFileReferenceClick}
                disabled={disabled || isSubmitting || isUploadingPaste
                    || fileReferenceBusy}
                aria-label="引用本地文件路径"
                title={fileReferenceTitle}
                className="panel-control flex h-10 w-10 items-center justify-center shrink-0 rounded-[10px] text-t2 transition-interactive duration-fast hover:bg-hover2 hover:text-t1 focus-visible:outline-hidden focus-visible:ring-[3px] focus-visible:ring-accent2-ring disabled:opacity-50"
            >
                {fileReferenceBusy
                    ? <Loader2 size={18} className="animate-spin" />
                    : <Paperclip size={18} />}
            </button>
        )}
        {!runActive && !compacting && (
            <FileUpload
                onFiles={onFiles}
                accept="image/*"
                disabled={disabled || isSubmitting || isUploadingPaste || imageCapability !== 'ready' || maxImages <= 0}
                title={imageCapabilityMessage}
            />
        )}
        {imageCapability === 'unavailable' && !runActive && !compacting && <button type="button" className="panel-control px-2 text-xs text-t2" onClick={onRetryImages} disabled={disabled || isSubmitting} aria-label="重新加载图片能力">重试图片能力</button>}
        {asrAvailable && !runActive && !compacting && (
            <VoiceInputButton
                onTranscript={onVoiceTranscript}
                disabled={disabled || isSubmitting || isUploadingPaste
                   }
            />
        )}
    </>
);

export default PromptToolbar;
