import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import ImageBlock from './ImageBlock';

describe('ImageBlock', () => {
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
