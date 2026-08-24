/**
 * HelpPanel — /help 命令结果美化渲染组件
 *
 * 按分组展示所有可用命令，支持折叠/展开
 */

import React, { useState } from 'react';
import { Terminal, ChevronDown, ChevronRight, HelpCircle } from 'lucide-react';

interface CommandItem {
    name: string;
    description: string;
    aliases: string[];
}

interface CommandGroup {
    title: string;
    titleZh: string;
    commands: CommandItem[];
}

interface HelpPanelProps {
    groups: CommandGroup[];
    total: number;
}

const groupColors: Record<string, { border: string; badge: string; badgeText: string }> = {
    'Local Commands':       { border: 'border-blue-700/30',  badge: 'bg-blue-900/40',  badgeText: 'text-blue-400' },
    'Interactive Commands': { border: 'border-purple-700/30', badge: 'bg-purple-900/40', badgeText: 'text-purple-400' },
    'Prompt Commands':      { border: 'border-green-700/30',  badge: 'bg-green-900/40',  badgeText: 'text-green-400' },
};

const defaultColor = { border: 'border-gray-700/30', badge: 'bg-gray-900/40', badgeText: 'text-gray-400' };

export const HelpPanel: React.FC<HelpPanelProps> = ({ groups, total }) => {
    const [expandedGroups, setExpandedGroups] = useState<Set<string>>(
        new Set(groups.map(g => g.title))
    );

    const toggleGroup = (title: string) => {
        setExpandedGroups(prev => {
            const next = new Set(prev);
            if (next.has(title)) next.delete(title);
            else next.add(title);
            return next;
        });
    };

    return (
        <div className="rounded-lg border border-gray-700/50 bg-gray-900/50 overflow-hidden max-w-2xl">
            {/* Header */}
            <div className="px-4 py-3 border-b border-gray-700/50 flex items-center justify-between">
                <div className="flex items-center gap-2">
                    <HelpCircle size={16} className="text-blue-400" />
                    <span className="text-sm font-medium text-gray-200">可用命令</span>
                </div>
                <span className="text-xs text-gray-500">
                    共 {total} 个命令 · 输入 <code className="px-1.5 py-0.5 bg-gray-800 rounded text-gray-400 font-mono">/help &lt;命令名&gt;</code> 查看详情
                </span>
            </div>

            {/* Command Groups */}
            <div className="divide-y divide-gray-800/50">
                {groups.map(group => {
                    const color = groupColors[group.title] ?? defaultColor;
                    const isExpanded = expandedGroups.has(group.title);

                    return (
                        <div key={group.title}>
                            {/* Group header */}
                            <button
                                onClick={() => toggleGroup(group.title)}
                                className="w-full flex items-center gap-2 px-4 py-2.5 hover:bg-gray-800/30 transition-colors text-left"
                            >
                                {isExpanded
                                    ? <ChevronDown size={14} className="text-gray-500" />
                                    : <ChevronRight size={14} className="text-gray-500" />
                                }
                                <span className={`text-xs font-medium px-2 py-0.5 rounded ${color.badge} ${color.badgeText}`}>
                                    {group.titleZh}
                                </span>
                                <span className="text-xs text-gray-500">
                                    {group.commands.length} 个命令
                                </span>
                            </button>

                            {/* Command list */}
                            {isExpanded && (
                                <div className="px-4 pb-2">
                                    <div className={`rounded-md border ${color.border} overflow-hidden`}>
                                        <table className="w-full text-xs">
                                            <tbody>
                                                {group.commands.map(cmd => (
                                                    <tr key={cmd.name} className="border-b border-gray-800/30 last:border-b-0 hover:bg-gray-800/20 transition-colors">
                                                        <td className="py-1.5 pl-3 pr-2 w-[180px]">
                                                            <div className="flex items-center gap-1.5">
                                                                <Terminal size={11} className="text-gray-600 flex-shrink-0" />
                                                                <code className="text-blue-400 font-mono font-medium">
                                                                    /{cmd.name}
                                                                </code>
                                                            </div>
                                                            {cmd.aliases.length > 0 && (
                                                                <div className="text-gray-600 ml-4 mt-0.5">
                                                                    {cmd.aliases.join(', ')}
                                                                </div>
                                                            )}
                                                        </td>
                                                        <td className="py-1.5 pr-3 text-gray-400">
                                                            {cmd.description}
                                                        </td>
                                                    </tr>
                                                ))}
                                            </tbody>
                                        </table>
                                    </div>
                                </div>
                            )}
                        </div>
                    );
                })}
            </div>
        </div>
    );
};
