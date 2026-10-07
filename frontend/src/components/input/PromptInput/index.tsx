import { useSessionStore } from '@/store/sessionStore';
import { useFeatureFlagStore } from '@/store/featureFlagStore';
import { useAppUiStore } from '@/store/appUiStore';
import { GlassMaterial } from '@/components/theme/GlassMaterial';
/**
 * PromptInput — 用户输入组件（容器：对外接口 + 形态分发）
 *
 * SPEC: §8.2.6a.7 PromptInput 完整交互实现
 * 核心功能: 1.多行输入(Shift+Enter 换行/Enter 提交) 2.命令面板(/ 补全, Ctrl+K 全局)
 * 3.历史导航(ArrowUp/Down) 4.附件(拖拽+按钮) 5.中断(Ctrl+C 无选中时) 6.IME 保护
 *
 * §8.3.1 拆分（零行为变化）：逻辑见 usePromptState.ts（组合
 * usePromptAttachments.ts / useLocalFileReference.ts）；
 * UI 见 PromptTextarea / PromptToolbar / PromptSendButton。
 */

import React from 'react';
import type {
    Command,
    SubmitEvent,
    Message,
} from '@/types';
import CommandPalette from '../CommandPalette';
import { FileAutoComplete } from '../FileAutoComplete';
import { useResponsive } from '@/hooks/useResponsive';
import { usePromptState } from './usePromptState';
import PromptTextarea from './PromptTextarea';
import { PromptAttachmentBar, PromptToolbar } from './PromptToolbar';
import PromptSendButton from './PromptSendButton';
import MobilePromptBar from './MobilePromptBar';
import { ModelChip, PermissionModeChip } from './PromptComposerChips';
import { DensitySwitch } from './DensitySwitch';
import { SessionStatusCapsule } from '@/components/status/SessionStatusCapsule';

interface PromptInputProps {
    sessionId?: string | null;
    onSubmit: (event: SubmitEvent) => Promise<boolean>;
    onSlashCommand: (command: string, skillId?: string) => Promise<boolean>;
    onInterrupt: () => void;
    onImmediateInterrupt?: () => void;
    disabled: boolean;
    runActive: boolean;
    compacting: boolean;
    permissionMode: string;
    messages: Message[];
    commands: Command[];
}

const PromptInput: React.FC<PromptInputProps> = (props) => {
    const { runActive, compacting, commands } = props;
    // §8.3.1-③ 形态分发：<768 移动胶囊形态（MobilePromptBar），其余桌面卡片形态。
    // usePromptState 在本组件顶层仅调用一次，产物同时喂给两种形态；
    // 形态切换只是同一实例内的条件渲染，输入状态不丢失。
    const { isMobile } = useResponsive();
    const activeSessionId = useSessionStore(state => state.sessionId);
    const purpose = useSessionStore(state => state.purpose);
    const activityEnabled = useFeatureFlagStore(state => state.flags.APOS_ACTIVITY_STREAM);
    const s = usePromptState({ ...props, disabled: props.disabled || purpose === 'mcp', sessionId: props.sessionId === undefined ? activeSessionId : props.sessionId });
    const { promptAttachments: a, localFileReference: f } = s;

    if (purpose === 'mcp') return <div className="chat-composer-surface space-y-2 rounded-panel border border-hairline bg-surfacev2 p-4 text-sm text-t2">
        <p role="status">MCP 专用会话 · 固定 DEFAULT 权限。此处查看外部工具活动和确认请求，不接收普通聊天或模型命令。</p>
        <button type="button" disabled={!activityEnabled} className="panel-control rounded border border-hairline px-3 py-2 disabled:opacity-50" onClick={() => {
            useAppUiStore.getState().requestVisualizationTab('apos');
            useAppUiStore.getState().setMobileNavTab('apos');
        }}>查看 Activity 与权限请求</button>
        {!activityEnabled && <p>Activity 视图已在功能设置中关闭；新的权限请求仍通过确认弹窗处理。</p>}
    </div>;
    return (
        <div
            className="chat-composer-surface glass-surface relative"
            onDrop={a.handleDrop}
            onDragOver={e => e.preventDefault()}
        >
            <GlassMaterial kind="control" interactive />
            {s.vimMode && <div role="status" className="px-3 pt-1 text-xs font-mono text-t2">VIM · {s.vimMode.toUpperCase()}</div>}
            {s.pendingChord && <div role="status" className="px-3 pt-1 text-xs text-t2">{s.pendingChord} … 等待和弦下一键（Esc 取消）</div>}
            {/* File auto-complete (@trigger) */}
            {s.showFileComplete && (
                <FileAutoComplete
                    query={s.fileQuery}
                    onSelect={s.handleFileCompleteSelect}
                    onClose={() => s.setShowFileComplete(false)} />
            )}

            {/* Slash command palette */}
            {s.showCommands && !runActive && !compacting && (
                <CommandPalette
                    commands={commands}
                    filter={s.input.slice(1)}
                    onSelect={(cmd, skillId) => { void s.submitSlashCommand('/' + cmd, true, skillId); }}
                    onClose={() => s.setShowCommands(false)} />
            )}

            {/* Global command palette (Ctrl+K) */}
            {s.showGlobalPalette && !runActive && !compacting && (
                <CommandPalette
                    commands={commands}
                    filter=""
                    onSelect={(cmd, skillId) => { void s.submitSlashCommand('/' + cmd, false, skillId); }}
                    onClose={() => s.setShowGlobalPalette(false)}
                    isGlobal />
            )}

            {isMobile ? (
                /* 移动胶囊形态（§7.6 移动态）：快捷条 + 胶囊条/展开卡片 */
                <MobilePromptBar
                    state={s}
                    runActive={runActive}
                    compacting={compacting}
                    disabled={props.disabled}
                    onInterrupt={props.onInterrupt}
                />
            ) : (
                /* 桌面卡片形态（P2b-1 既有 JSX，零行为变化） */
                <>
                    <PromptAttachmentBar
                        attachments={a.attachments}
                        imageCount={a.imageCount}
                        maxImages={a.maxImages}
                        localFiles={f.localFiles}
                        onRemoveAttachment={a.removeAttachment}
                        setLocalFiles={f.setLocalFiles}
                    />

                    {/* Input area */}
                    <div className="flex items-end gap-2">
                        <PromptTextarea
                            value={s.input}
                            onValueChange={s.setInput}
                            onCursorChange={s.syncCursorPos}
                            onAtQueryChange={s.handleAtQueryChange}
                            onSlashIntent={s.handleSlashIntent}
                            onKeyDown={s.handleKeyDown}
                            onPaste={a.handlePaste}
                            textareaRef={s.textareaRef}
                            compacting={compacting}
                            runActive={runActive}
                            disabled={props.disabled || compacting || s.isSubmitting
                                || a.isUploadingPaste}
                        />
                        <PromptToolbar
                            runActive={runActive}
                            compacting={compacting}
                            disabled={props.disabled}
                            isSubmitting={s.isSubmitting}
                            isUploadingPaste={a.isUploadingPaste}
                            fileReferenceBusy={f.fileReferenceBusy}
                            fileReferenceTitle={f.fileReferenceTitle}
                            asrAvailable={s.asrAvailable}
                            maxImages={a.maxImages}
                            onFileReferenceClick={f.handleFileReferenceClick}
                            onFiles={a.handleFiles}
                            onVoiceTranscript={s.handleVoiceTranscript}
                        />
                        <PromptSendButton
                            runActive={runActive}
                            sendDisabled={props.disabled || compacting || s.isSubmitting
                                || a.isUploadingPaste || f.fileReferenceBusy
                                || (!s.input.trim() && a.attachments.length === 0
                                    && f.localFiles.length === 0
)}
                            stopDisabled={props.disabled || s.isSubmitting}
                            onSend={() => { void s.handleSubmit(); }}
                            onInterrupt={props.onInterrupt}
                        />
                    </div>

                    {/* composer-row：权限/模型 mini-chip + 显示方式分段控件 + 键盘提示
                        （仅桌面分支渲染；显示方式切换自消息区工具条迁入） */}
                    <div className="mt-2 flex items-center gap-2">
                        {/* 会话状态胶囊：与 Header 右簇同款组件，独立于权限 chip 并排放置 */}
                        <SessionStatusCapsule />
                        <PermissionModeChip />
                        <ModelChip />
                        <DensitySwitch />
                        <span className="flex-1" />
                        <span className="text-[13px] text-t2">⏎ 发送&ensp;·&ensp;⇧⏎ 换行</span>
                    </div>
                </>
            )}
        </div>
    );
};

export default React.memo(PromptInput);
