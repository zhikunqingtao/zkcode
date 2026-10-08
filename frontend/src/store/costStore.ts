/**
 * CostStore — 费用状态管理
 * SPEC: §8.3 Store #5
 * 持久化: 否 (由 #15 cost_update 权威推送)
 */

import { create } from 'zustand';
import { immer } from 'zustand/middleware/immer';
import { subscribeWithSelector } from 'zustand/middleware';
import type { PricingStatus, Usage } from '@/types';

export interface CostStoreState {
    sessionCost: number;
    totalCost: number;
    usage: Usage;
    /** False when at least one physical model call lacks authoritative usage. */
    usageComplete: boolean;
    sessionPricingStatus: PricingStatus;
    totalPricingStatus: PricingStatus;

    updateCost: (data: {
        sessionCost: number;
        totalCost: number;
        usage?: Usage;
        usageComplete?: boolean;
        sessionPricingStatus?: PricingStatus;
        totalPricingStatus?: PricingStatus;
    }) => void;
    resetSessionCost: () => void;
}

export const useCostStore = create<CostStoreState>()(
    subscribeWithSelector(immer((set) => ({
        sessionCost: 0,
        totalCost: 0,
        usage: { inputTokens: 0, outputTokens: 0, cacheReadInputTokens: 0, cacheCreationInputTokens: 0 },
        usageComplete: true,
        sessionPricingStatus: 'unavailable',
        totalPricingStatus: 'unavailable',

        updateCost: (data) => set(d => {
            d.sessionCost = data.sessionCost;
            d.totalCost = data.totalCost;
            if (data.usage) d.usage = data.usage;
            d.usageComplete = data.usageComplete ?? d.usageComplete;
            d.sessionPricingStatus = normalizePricingStatus(data.sessionPricingStatus);
            d.totalPricingStatus = normalizePricingStatus(data.totalPricingStatus);
        }),
        resetSessionCost: () => set(d => {
            d.sessionCost = 0;
            d.sessionPricingStatus = 'unavailable';
            d.usage = { inputTokens: 0, outputTokens: 0, cacheReadInputTokens: 0, cacheCreationInputTokens: 0 };
            d.usageComplete = true;
        }),
    })))
);

function normalizePricingStatus(status: unknown): PricingStatus {
    return status === 'known' || status === 'unknown' ? status : 'unavailable';
}
