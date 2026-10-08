import { render } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { SearchResultRenderer } from './SearchResultRenderer';

describe('Grep output is untrusted text', () => {
    it.each([undefined, 'img', '<img'])('does not interpret markup with query %s', query => {
        const payload = '<img src=x onerror="alert(1)"><svg onload="alert(2)">';
        const { container } = render(<SearchResultRenderer content={`src/a.ts:12:${payload}`} query={query} />);
        expect(container.querySelector('img,svg,script')).toBeNull();
        expect(container.textContent).toContain(payload);
    });
    it('highlights literal regex characters without changing their text', () => {
        const { container } = render(<SearchResultRenderer content={'a:1:a[b] A[B] a.b'} query="a[b]" />);
        expect([...container.querySelectorAll('mark')].map(mark => mark.textContent)).toEqual(['a[b]', 'A[B]']);
    });
});
