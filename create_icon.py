#!/usr/bin/env python3
"""Create an icon for the Data Wrangler application.
Created by Mohamad W."""

from PIL import Image, ImageDraw, ImageFont
import os

def create_icon():
    """Create a simple icon for the Data Wrangler application."""
    
    # Create a 64x64 image with a blue background
    size = 64
    img = Image.new('RGBA', (size, size), (31, 119, 180, 255))  # Blue background
    draw = ImageDraw.Draw(img)
    
    # Draw a white wrench icon
    # Wrench handle
    draw.rectangle([20, 15, 25, 50], fill='white')
    
    # Wrench head (open end)
    draw.rectangle([15, 20, 35, 25], fill='white')
    draw.rectangle([15, 35, 35, 40], fill='white')
    
    # Wrench head (closed end)
    draw.rectangle([35, 25, 45, 40], fill='white')
    
    # Add a small gear/cog in the corner
    center_x, center_y = 50, 15
    radius = 8
    
    # Draw gear teeth
    for i in range(8):
        angle = i * 45
        x1 = center_x + radius * 0.7 * cos(radians(angle))
        y1 = center_y + radius * 0.7 * sin(radians(angle))
        x2 = center_x + radius * cos(radians(angle))
        y2 = center_y + radius * sin(radians(angle))
        draw.line([x1, y1, x2, y2], fill='white', width=2)
    
    # Draw center circle
    draw.ellipse([center_x - 3, center_y - 3, center_x + 3, center_y + 3], fill='white')
    
    # Save as ICO file
    img.save('data_wrangler.ico', format='ICO', sizes=[(16, 16), (32, 32), (48, 48), (64, 64)])
    print("Icon created: data_wrangler.ico")
    
    # Also save as PNG for reference
    img.save('data_wrangler.png', format='PNG')
    print("PNG version created: data_wrangler.png")

def cos(angle):
    """Simple cosine function."""
    import math
    return math.cos(angle)

def sin(angle):
    """Simple sine function."""
    import math
    return math.sin(angle)

def radians(degrees):
    """Convert degrees to radians."""
    import math
    return math.radians(degrees)

if __name__ == "__main__":
    try:
        create_icon()
    except ImportError:
        print("PIL (Pillow) not found. Installing...")
        import subprocess
        import sys
        subprocess.check_call([sys.executable, "-m", "pip", "install", "Pillow"])
        create_icon()
