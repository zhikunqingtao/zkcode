/**
 * SettingsPanel — 设置面板
 * SPEC: §8.2.6a.11 SettingsPanel
 *
 * 包含: 主题设置、模型选择、权限模式、快捷键等
 */

import React, { useEffect, useState } from 'react';
import { X, Sun, Keyboard, Shield, Globe, KeyRound } from 'lucide-react';
import { useConfigStore } from '@/store/configStore';
import { useSessionStore } from '@/store/sessionStore';
import { useModelStore } from '@/store/modelStore';
import { ThemePicker } from '@/components/theme/ThemePicker';
import { useSessionPermissionSelection } from '@/hooks/useSessionPermissionSelection';
import { useSessionModelSelection } from '@/hooks/useSessionModelSelection';
import { SpaceshipFxControls } from '@/components/theme/SpaceshipFxControls';
import { InkHavocFxControls } from '@/components/theme/InkHavocFxControls';
import { JellyFxControls } from '@/components/theme/JellyFxControls';
import { ApiKeysTab } from '@/components/settings/ApiKeysTab';
import { SessionExecutionControls } from '@/components/settings/SessionExecutionControls';
import { KeybindingsEditor } from '@/components/settings/KeybindingsEditor';
import type { PermissionMode } from '@/types';

interface SettingsPanelProps {
    onClose: () => void;
}

export const SettingsPanel: React.FC<SettingsPanelProps> = ({ onClose }) => {
    const [activeSection, setActiveSection] = useState<'general' | 'api-keys'>('general');
    const { theme, locale, setLocale, defaultModel, asrContextEnabled, setAsrContextEnabled, saveConfig } = useConfigStore();
    const { model } = useSessionStore();
    const permissionSelection = useSessionPermissionSelection();
    const { permissionMode, selectMode: handlePermissionModeChange } = permissionSelection;
    const {
        models: availableModels,
        loaded: modelsLoaded,
        loading: modelsLoading,
        error: modelsError,
        fetchModels,
    } = useModelStore();
    const hasBoundSession = !permissionSelection.disabled;

    useEffect(() => {
        if (!modelsLoaded) void fetchModels();
    }, [fetchModels, modelsLoaded]);

    const modelSelection = useSessionModelSelection();

    return (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-xs">
            <div
                role="dialog"
                aria-modal="true"
                aria-labelledby="settings-dialog-title"
                className="w-full max-w-2xl mx-4 max-h-[80vh] rounded-xl border border-[var(--border)]
                           bg-[var(--bg-primary)] shadow-2xl overflow-hidden flex flex-col"
            >
                {/* Header */}
                <div className="px-6 py-4 border-b border-[var(--border)] flex items-center justify-between">
                    <h2 id="settings-dialog-title" className="text-lg font-semibold text-[var(--text-primary)]">设置</h2>
                    <button
                        type="button"
                        onClick={onClose}
                        aria-label="关闭设置"
                        className="p-2 rounded-lg hover:bg-[var(--bg-hover)] text-[var(--text-muted)]"
                    >
                        <X className="w-5 h-5" />
                    </button>
                </div>

                <div
                    role="tablist"
                    aria-label="设置分类"
                    className="flex gap-1 px-6 pt-4 border-b border-[var(--border)]"
                >
                    <SettingsTabButton
                        id="settings-general-tab"
                        panelId="settings-general-panel"
                        label="常规"
                        selected={activeSection === 'general'}
                        onClick={() => setActiveSection('general')}
                    />
                    <SettingsTabButton
                        id="settings-api-keys-tab"
                        panelId="settings-api-keys-panel"
                        label="API Keys"
                        selected={activeSection === 'api-keys'}
                        onClick={() => setActiveSection('api-keys')}
                        icon={<KeyRound className="w-4 h-4" aria-hidden="true" />}
                    />
                </div>

                {/* Content */}
                {activeSection === 'general' ? (
                    <div
                        id="settings-general-panel"
                        role="tabpanel"
                        aria-labelledby="settings-general-tab"
                        className="flex-1 overflow-y-auto p-6 space-y-8"
                    >
                    {/* Theme Section */}
                    <section>
                        <h3 className="text-sm font-medium text-[var(--text-secondary)] mb-3 flex items-center gap-2">
                            <Sun className="w-4 h-4" />
                            主题
                        </h3>
                        <ThemePicker />
                        {theme.mode === 'spaceship' && <SpaceshipFxControls />}
                        {(theme.mode === 'ink-havoc' || theme.mode === 'ink-havoc-night') && <InkHavocFxControls />}
                        {theme.mode === 'jelly' && <JellyFxControls />}
                    </section>

                    {/* Model Section */}
                    <section>
                        <h3 className="text-sm font-medium text-[var(--text-secondary)] mb-3 flex items-center gap-2">
                            <Globe className="w-4 h-4" />
                            当前会话模型
                        </h3>
                        <select
                            aria-label="当前会话模型"
                            value={model || ''}
                            disabled={modelSelection.disabled}
                            onChange={(e) => modelSelection.selectModel(e.target.value)}
                            className="w-full px-3 py-2 rounded-lg border border-[var(--border)]
                                bg-[var(--bg-secondary)] text-[var(--text-primary)]
                                focus:outline-hidden focus:ring-2 focus:ring-blue-500"
                        >
                            {!availableModels.length && (
                                <option value="">
                                    {modelsLoading ? '模型加载中…'
                                        : modelsError ? '模型列表加载失败' : '暂无可用模型'}
                                </option>
                            )}
                            {availableModels.map((availableModel) => (
                                <option key={availableModel.id} value={availableModel.id}>
                                    {availableModel.displayName}
                                </option>
                            ))}
                        </select>
                        {modelsError && (
                            <button
                                type="button"
                                onClick={() => void fetchModels()}
                                className="mt-2 text-sm text-blue-500 hover:underline"
                            >
                                重新加载模型列表
                            </button>
                        )}

                        <label className="mt-4 block text-sm text-t2">新会话默认模型</label>
                        <select aria-label="新会话默认模型" value={defaultModel ?? ''}
                            disabled={modelsLoading || availableModels.length === 0}
                            onChange={e => void saveConfig({ defaultModel: e.target.value })}
                            className="mt-2 w-full rounded-lg border border-hairline bg-sunken2 px-3 py-2 text-t1">
                            {availableModels.map(item => <option key={item.id} value={item.id}>{item.displayName}</option>)}
                        </select>
                        <label className="mt-4 flex items-start gap-2 text-sm text-t2">
                            <input type="checkbox" checked={asrContextEnabled} onChange={e => setAsrContextEnabled(e.target.checked)} />
                            语音识别使用最近 3 轮对话作为上下文（可选）
                        </label>
                        <SessionExecutionControls />
                    </section>

                    {/* Permission Section */}
                    <section>
                        <h3 className="text-sm font-medium text-[var(--text-secondary)] mb-3 flex items-center gap-2">
                            <Shield className="w-4 h-4" />
                            权限模式
                        </h3>
                        <div className="space-y-2">
                            <PermissionOption
                                mode="default"
                                label="默认模式"
                                description="标准权限控制"
                                selected={permissionMode === 'default'}
                                onClick={() => handlePermissionModeChange('default')}
                                disabled={!hasBoundSession}
                            />
                            <PermissionOption
                                mode="plan"
                                label="计划模式"
                                description="先制定计划再执行"
                                selected={permissionMode === 'plan'}
                                onClick={() => handlePermissionModeChange('plan')}
                                disabled={!hasBoundSession}
                            />
                            <PermissionOption
                                mode="accept_edits"
                                label="接受编辑"
                                description="自动接受编辑操作"
                                selected={permissionMode === 'accept_edits'}
                                onClick={() => handlePermissionModeChange('accept_edits')}
                                disabled={!hasBoundSession}
                            />
                            <PermissionOption
                                mode="dont_ask"
                                label="无需询问"
                                description="不弹窗，需要确认的操作自动拒绝"
                                selected={permissionMode === 'dont_ask'}
                                onClick={() => handlePermissionModeChange('dont_ask')}
                                disabled={!hasBoundSession}
                            />
                            <PermissionOption
                                mode="auto_approve"
                                label="完全访问权限"
                                description="自动批准所有工具权限请求，允许请求工作区外文件和公共互联网；仍执行系统安全与部署限制"
                                selected={permissionMode === 'auto_approve'}
                                onClick={() => handlePermissionModeChange('auto_approve')}
                                disabled={!hasBoundSession}
                                warning
                            />
                        </div>
                        {!hasBoundSession && (
                            <p className="mt-2 text-xs text-[var(--text-muted)]">
                                请先创建或选择会话后再设置权限模式。
                            </p>
                        )}
                    </section>

                    {/* Language Section */}
                    <section>
                        <h3 className="text-sm font-medium text-[var(--text-secondary)] mb-3 flex items-center gap-2">
                            <Globe className="w-4 h-4" />
                            语言
                        </h3>
                        <select
                            value={locale}
                            onChange={(e) => setLocale(e.target.value)}
                            className="w-full px-3 py-2 rounded-lg border border-[var(--border)]
                                bg-[var(--bg-secondary)] text-[var(--text-primary)]
                                focus:outline-hidden focus:ring-2 focus:ring-blue-500"
                        >
                            <option value="zh-CN">简体中文</option>
                            <option value="zh-TW">繁體中文</option>
                            <option value="en-US">English</option>
                            <option value="ja-JP">日本語</option>
                        </select>
                    </section>

                    {/* Shortcuts Section */}
                    <section>
                        <h3 className="text-sm font-medium text-[var(--text-secondary)] mb-3 flex items-center gap-2">
                            <Keyboard className="w-4 h-4" />
                            快捷键
                        </h3>
                        <KeybindingsEditor />
                        <div className="space-y-2 text-sm">
                            <ShortcutItem keys={['Shift', 'Enter']} description="换行" />
                            <ShortcutItem keys={['/']} description="打开命令面板" />
                            <ShortcutItem keys={['Esc']} description="取消/关闭" />
                            <ShortcutItem keys={['Ctrl', 'C']} description="中断生成" />
                        </div>
                    </section>
                    </div>
                ) : (
                    <div
                        id="settings-api-keys-panel"
                        role="tabpanel"
                        aria-labelledby="settings-api-keys-tab"
                        className="flex-1 overflow-y-auto p-6"
                    >
                        <ApiKeysTab />
                    </div>
                )}

                {/* Footer */}
                <div className="px-6 py-4 border-t border-[var(--border)] flex justify-end">
                    <button
                        type="button"
                        onClick={onClose}
                        className="px-4 py-2 rounded-lg bg-blue-600 hover:bg-blue-700 text-white text-sm"
                    >
                        完成
                    </button>
                </div>
            </div>
        </div>
    );
};

function SettingsTabButton({
    id,
    panelId,
    label,
    selected,
    onClick,
    icon,
}: {
    id: string;
    panelId: string;
    label: string;
    selected: boolean;
    onClick: () => void;
    icon?: React.ReactNode;
}) {
    return (
        <button
            id={id}
            type="button"
            role="tab"
            aria-selected={selected}
            aria-controls={panelId}
            tabIndex={selected ? 0 : -1}
            onClick={onClick}
            className={`flex items-center gap-2 px-4 py-2 -mb-px border-b-2 text-sm transition-colors
                ${selected
                    ? 'border-blue-500 text-blue-500'
                    : 'border-transparent text-[var(--text-muted)] hover:text-[var(--text-primary)]'
                }`}
        >
            {icon}
            {label}
        </button>
    );
}

// Permission Option Component
function PermissionOption({
    label,
    description,
    selected,
    onClick,
    disabled,
    warning = false,
}: {
    mode: PermissionMode;
    label: string;
    description: string;
    selected: boolean;
    onClick: () => void;
    disabled: boolean;
    warning?: boolean;
}) {
    return (
        <button
            onClick={onClick}
            disabled={disabled}
            className={`w-full px-4 py-3 rounded-lg border text-left transition-all
                ${disabled ? 'cursor-not-allowed opacity-50' : ''}
                ${selected
                    ? warning ? 'border-orange-500 bg-orange-500/10' : 'border-blue-500 bg-blue-500/10'
                    : warning ? 'border-orange-500/60 hover:bg-orange-500/10'
                        : 'border-[var(--border)] hover:border-blue-500/50 hover:bg-[var(--bg-hover)]'
                }`}
        >
            <div className={`font-medium ${warning ? 'text-orange-500'
                : selected ? 'text-blue-500' : 'text-[var(--text-primary)]'}`}>
                {label}
            </div>
            <div className="text-sm text-[var(--text-muted)]">{description}</div>
        </button>
    );
}

// Shortcut Item Component
function ShortcutItem({ keys, description }: { keys: string[]; description: string }) {
    return (
        <div className="flex items-center justify-between py-1">
            <span className="text-[var(--text-secondary)]">{description}</span>
            <div className="flex items-center gap-1">
                {keys.map((key, index) => (
                    <React.Fragment key={key}>
                        <kbd className="px-2 py-0.5 bg-[var(--bg-secondary)] border border-[var(--border)]
                            rounded-sm text-xs text-[var(--text-primary)]">
                            {key}
                        </kbd>
                        {index < keys.length - 1 && <span className="text-[var(--text-muted)]">+</span>}
                    </React.Fragment>
                ))}
            </div>
        </div>
    );
}

export default SettingsPanel;
