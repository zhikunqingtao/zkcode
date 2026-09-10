/**
 * UserMessage — 用户消息渲染组件
 *
 * SPEC: §8.2.1 UserMessage, §8.2.4J MessageType='user'
 * 渲染用户输入文本 + 附件预览。
 */

import React from 'react';
import { User } from 'lucide-react';
import type { Message, ContentBlock } from '@/types';
import TextBlock from './TextBlock';
import ImageBlock from './ImageBlock';

interface UserMessageProps {
    message: Extract<Message, { type: 'user' }>;
}

const UserMessage: React.FC<UserMessageProps> = ({ message }) => {
    const visibleBlocks = message.content.filter(
        block => block.type === 'text' || block.type === 'image',
    );
    // tool_result 是模型协议内部回执，不是人类发送的消息。
    if (visibleBlocks.length === 0) return null;
    return (
        <div className="user-message flex gap-3 px-4 py-3">
            {/* Avatar */}
            <div className="flex-shrink-0 w-7 h-7 rounded-full bg-blue-600 flex items-center justify-center">
                <User size={14} className="text-white" />
            </div>

            {/* Content */}
            <div className="flex-1 min-w-0">
                <div className="text-xs text-[var(--text-secondary)] mb-1 font-medium">You</div>
                <div className="text-sm text-[var(--text-primary)]">
                    {visibleBlocks.map((block, i) => (
                        <ContentBlockRenderer key={i} block={block} />
                    ))}
                </div>
            </div>
        </div>
    );
};

const ContentBlockRenderer: React.FC<{ block: ContentBlock }> = ({ block }) => {
    switch (block.type) {
        case 'text':
            return <TextBlock text={block.text} />;
        case 'image':
            return <ImageBlock base64Data={block.base64Data} src={block.url} mediaType={block.mediaType} />;
        default:
            return null;
    }
};

export default React.memo(UserMessage);
