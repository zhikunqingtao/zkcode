import { useState } from 'react';
import { Dialog, Tabs } from '@/components/ui';
import { McpCapabilityPanel } from '@/components/settings/McpCapabilityPanel';
import { PromptsTab } from '@/components/settings/PromptsTab';
import { McpServicesPanel } from './McpServicesPanel';
export function McpManagementPage({onClose}: {onClose: () => void}) {
    const [tab, setTab] = useState('tools');
    return <Dialog open title="MCP 管理" onClose={onClose} className="max-w-5xl max-h-[85vh] overflow-auto">
        <Tabs aria-label="MCP 内容" items={[{value:'tools',label:'工具'},{value:'services',label:'服务'},{value:'prompts',label:'提示模板'}]} value={tab} onValueChange={setTab} />
        <div className="p-4">{tab === 'tools' ? <McpCapabilityPanel /> : tab === 'services' ? <McpServicesPanel /> : <PromptsTab />}</div>
    </Dialog>;
}
