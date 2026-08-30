import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import ImageBlock from './ImageBlock';

describe('ImageBlock', () => {
    it('clears a previous load error when the source changes', () => {
        const { rerender } = render(<ImageBlock src="https://images.example.test/bad.png" alt="preview" />);
        fireEvent.error(screen.getByAltText('preview'));
        expect(screen.getByText('Failed to load image')).toBeInTheDocument();

        rerender(<ImageBlock src="https://images.example.test/good.png" alt="preview" />);
        expect(screen.getByAltText('preview')).toHaveAttribute('src', 'https://images.example.test/good.png');
        expect(screen.queryByText('Failed to load image')).not.toBeInTheDocument();
    });
});
