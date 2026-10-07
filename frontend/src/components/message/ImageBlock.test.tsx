import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { copyImageToClipboard } from '@/utils/messageContent';
import ImageBlock from './ImageBlock';

vi.mock('@/utils/messageContent', () => ({ copyImageToClipboard: vi.fn().mockResolvedValue(undefined) }));

describe('ImageBlock', () => {
    it('portals zoom outside transformed ancestors and copies the displayed source', async () => {
        const { container } = render(<div style={{ transform: 'scale(1)' }}><ImageBlock src="blob:authorized-preview" alt="local" /></div>);
        fireEvent.click(screen.getByRole('button', { name: 'Zoom image' }));
        const close = screen.getByRole('button', { name: 'Close zoom' });
        expect(container.contains(close)).toBe(false);
        expect(screen.getAllByAltText('local').every(image => image.getAttribute('referrerpolicy') === 'no-referrer')).toBe(true);
        fireEvent.click(screen.getByRole('button', { name: 'Copy image' }));
        expect(copyImageToClipboard).toHaveBeenCalledWith({ url: 'blob:authorized-preview', mediaType: 'image/png' });
        await screen.findByRole('button', { name: 'Image copied' });
    });

    it('closes the enlarged image when the close button is clicked', () => {
        render(<ImageBlock src="https://images.example.test/preview.png" alt="preview" />);

        fireEvent.click(screen.getByRole('img', { name: 'preview' }));
        fireEvent.click(screen.getByRole('button', { name: 'Close zoom' }));

        expect(screen.queryByRole('button', { name: 'Close zoom' })).not.toBeInTheDocument();
    });

    it('clears a previous load error when the source changes', () => {
        const { rerender } = render(<ImageBlock src="https://images.example.test/bad.png" alt="preview" />);
        fireEvent.error(screen.getByAltText('preview'));
        expect(screen.getByText('Failed to load image')).toBeInTheDocument();

        rerender(<ImageBlock src="https://images.example.test/good.png" alt="preview" />);
        expect(screen.getByAltText('preview')).toHaveAttribute('src', 'https://images.example.test/good.png');
        expect(screen.queryByText('Failed to load image')).not.toBeInTheDocument();
    });
});
