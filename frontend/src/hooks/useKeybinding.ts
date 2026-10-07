import { useCallback, useEffect, useRef, useState } from 'react';
import { keyboardCombo } from '@/keyboard/shortcuts';

export type KeybindingContext =
  | 'global' | 'chat' | 'autocomplete' | 'confirmation' | 'help'
  | 'transcript' | 'history_search' | 'task' | 'theme_picker'
  | 'settings' | 'tabs' | 'scroll' | 'attachments' | 'footer'
  | 'message_selector' | 'message_actions' | 'diff_dialog'
  | 'model_picker' | 'select';

export interface KeyBinding {
  key: string;
  action: string;
  context: KeybindingContext;
  handler: () => void;
  enabled?: boolean;
  allowInInput?: boolean;
  when?: (event: KeyboardEvent) => boolean;
}
const activeContexts = new Map<KeybindingContext, number>();
export const CHORD_TIMEOUT_MS = 1200;
export function isContextActive(context: KeybindingContext): boolean {
  return context === 'global' || (activeContexts.get(context) ?? 0) > 0;
}

/** Contexts are reference counted: unmounting one panel cannot unregister another. */
export function useRegisterKeybindingContext(context: KeybindingContext, isActive = true) {
  useEffect(() => {
    if (!isActive) return;
    activeContexts.set(context, (activeContexts.get(context) ?? 0) + 1);
    return () => {
      const remaining = (activeContexts.get(context) ?? 1) - 1;
      if (remaining > 0) activeContexts.set(context, remaining);
      else activeContexts.delete(context);
    };
  }, [context, isActive]);
}

/** Dispatches configured shortcuts after React/editor handlers have had first refusal. */
export function useKeybinding(bindings: KeyBinding[]) {
  const bindingsRef = useRef(bindings);
  bindingsRef.current = bindings;
  const [pendingChord, setPendingChord] = useState<string | null>(null);
  const chordRef = useRef<string | null>(null);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const clearChord = useCallback(() => {
    if (timerRef.current) clearTimeout(timerRef.current);
    timerRef.current = null;
    chordRef.current = null;
    setPendingChord(null);
  }, []);

  useEffect(() => {
    const keydown = (event: KeyboardEvent) => {
      if (event.defaultPrevented || event.isComposing || event.keyCode === 229 || event.repeat) return;
      if (event.key === 'Escape' && chordRef.current) {
        clearChord(); event.preventDefault(); return;
      }
      const combo = keyboardCombo(event);
      if (!combo) return;
      const target = event.target;
      const editable = target instanceof HTMLElement && (target.matches('input,textarea,select') || target.isContentEditable);
      const eligible = bindingsRef.current.filter(binding => binding.enabled !== false
        && isContextActive(binding.context) && (!binding.when || binding.when(event))
        && (!editable || binding.allowInInput || event.ctrlKey || event.altKey || event.metaKey))
        .sort((a, b) => Number(a.context === 'global') - Number(b.context === 'global'));
      const sequence = chordRef.current ? `${chordRef.current} ${combo}` : combo;
      const match = eligible.find(binding => binding.key === sequence);
      clearChord();
      if (match) {
        event.preventDefault(); event.stopPropagation(); match.handler(); return;
      }
      if (eligible.some(binding => binding.key.startsWith(`${sequence} `))) {
        event.preventDefault(); event.stopPropagation();
        chordRef.current = sequence;
        setPendingChord(sequence);
        timerRef.current = setTimeout(clearChord, CHORD_TIMEOUT_MS);
      }
    };
    window.addEventListener('keydown', keydown);
    document.addEventListener('focusin', clearChord);
    window.addEventListener('blur', clearChord);
    return () => {
      window.removeEventListener('keydown', keydown);
      document.removeEventListener('focusin', clearChord);
      window.removeEventListener('blur', clearChord);
      if (timerRef.current) clearTimeout(timerRef.current);
      chordRef.current = null;
    };
  }, [clearChord]);
  return { pendingChord };
}
export default useKeybinding;
