import { renderHook } from '@testing-library/react';
import { expect, it } from 'vitest';
import { usePageExitGuard } from './usePageExitGuard';

it('guards browser exits only while mounted and removes its listener', () => {
    const { unmount } = renderHook(usePageExitGuard);
    const leaving = new Event('beforeunload', { cancelable: true });
    window.dispatchEvent(leaving);
    expect(leaving.defaultPrevented).toBe(true);
    unmount();
    const afterUnmount = new Event('beforeunload', { cancelable: true });
    window.dispatchEvent(afterUnmount);
    expect(afterUnmount.defaultPrevented).toBe(false);
});
