import axios from 'axios';
import sharp from 'sharp';
import { promises as fs } from 'fs';
import path from 'path';

interface ProductOptions {
  quality?: number;
  outputDir?: string;
  maxWidth?: number;
  maxHeight?: number;
  backgroundTolerance?: number;
  edgeSmoothing?: number;
  compressionLevel?: number;
}

interface ProcessingResult {
  success: boolean;
  outputPath?: string;
  error?: string;
  memoryUsed?: string;
}

export class ProductBackgroundRemover {
  private defaultOptions: ProductOptions = {
    quality: 85,
    outputDir: 'processed_products',
    maxWidth: 1200,      // Limit size to prevent memory issues
    maxHeight: 1200,
    backgroundTolerance: 25,
    edgeSmoothing: 1.5,
    compressionLevel: 6
  };

  /**
   * Remove background from product images with memory optimization
   */
  async removeProductBackground(
    imageUrl: string,
    outputFilename: string,
    options: ProductOptions = {}
  ): Promise<ProcessingResult> {
    const opts = { ...this.defaultOptions, ...options };
    const startMemory = process.memoryUsage().heapUsed;

    try {
      // Step 1: Download and get image info without loading full data
      const response = await axios.get(imageUrl, { responseType: 'arraybuffer' });
      const inputBuffer = Buffer.from(response.data);

      await fs.mkdir(opts.outputDir!, { recursive: true });
      const outputPath = path.join(opts.outputDir!, outputFilename);

      // Step 2: Get metadata first to check size
      const metadata = await sharp(inputBuffer).metadata();
      console.log(`📏 Original size: ${metadata.width}x${metadata.height}`);

      // Step 3: Resize if too large (memory optimization)
      let processWidth = metadata.width!;
      let processHeight = metadata.height!;
      
      if (processWidth > opts.maxWidth! || processHeight > opts.maxHeight!) {
        const ratio = Math.min(opts.maxWidth! / processWidth, opts.maxHeight! / processHeight);
        processWidth = Math.round(processWidth * ratio);
        processHeight = Math.round(processHeight * ratio);
        console.log(`📐 Resizing to: ${processWidth}x${processHeight}`);
      }

      // Step 4: Process in Sharp pipeline (memory efficient)
      let pipeline = sharp(inputBuffer)
        .resize(processWidth, processHeight, {
          kernel: sharp.kernel.lanczos3,
          withoutEnlargement: true
        });

      // Step 5: Use Sharp's built-in operations for efficiency
      const result = await pipeline
        .ensureAlpha()
        .raw()
        .toBuffer({ resolveWithObject: true });

      const { data, info } = result;
      console.log(`🔧 Processing ${info.width}x${info.height} image...`);

      // Step 6: Efficient background removal for products
      const processedData = this.removeProductBackgroundOptimized(
        data, 
        info.width, 
        info.height, 
        opts.backgroundTolerance!,
        opts.edgeSmoothing!
      );

      // Step 7: Create final image with optimal WebP settings
      await sharp(processedData, {
        raw: {
          width: info.width,
          height: info.height,
          channels: 4
        }
      })
      .webp({
        quality: opts.quality!,
        effort: opts.compressionLevel!,
        smartSubsample: true,
        mixed: true
      })
      .toFile(outputPath);

      const endMemory = process.memoryUsage().heapUsed;
      const memoryUsed = `${Math.round((endMemory - startMemory) / 1024 / 1024)}MB`;

      console.log(`✅ Successfully processed: ${outputPath} (Memory used: ${memoryUsed})`);
      
      // Force garbage collection hint
      if (global.gc) {
        global.gc();
      }

      return { success: true, outputPath, memoryUsed };

    } catch (error) {
      const errorMessage = `Error processing ${imageUrl}: ${error instanceof Error ? error.message : 'Unknown error'}`;
      console.error(`❌ ${errorMessage}`);
      return { success: false, error: errorMessage };
    }
  }

  /**
   * Optimized background removal specifically for product photography
   */
  private removeProductBackgroundOptimized(
    data: Buffer, 
    width: number, 
    height: number, 
    tolerance: number,
    smoothing: number
  ): Buffer {
    const resultData = Buffer.from(data);
    
    // Step 1: Find background regions starting from edges (products are usually centered)
    const backgroundMask = this.createProductMask(data, width, height, tolerance);
    
    // Step 2: Apply mask with gradient transitions
    for (let y = 0; y < height; y++) {
      for (let x = 0; x < width; x++) {
        const i = (y * width + x) * 4;
        const maskIndex = y * width + x;
        
        if (backgroundMask[maskIndex] > 0) {
          // Background pixel - apply alpha based on distance from edge
          const alpha = this.calculateEdgeAlpha(x, y, width, height, backgroundMask, smoothing);
          resultData[i + 3] = alpha;
        }
        // Foreground pixels keep original alpha
      }
    }
    
    return resultData;
  }

  /**
   * Create mask for product images (simpler than complex edge detection)
   */
  private createProductMask(data: Buffer, width: number, height: number, tolerance: number): Uint8Array {
    const mask = new Uint8Array(width * height);
    const visited = new Uint8Array(width * height);
    
    // Products are usually centered, so start flood fill from corners and edges
    const startPoints: [number, number][] = [];
    
    // Add corner points
    startPoints.push([0, 0], [width - 1, 0], [0, height - 1], [width - 1, height - 1]);
    
    // Add edge points (every 10th pixel to save memory)
    for (let x = 0; x < width; x += 10) {
      startPoints.push([x, 0], [x, height - 1]);
    }
    for (let y = 0; y < height; y += 10) {
      startPoints.push([0, y], [width - 1, y]);
    }
    
    // Flood fill from each starting point
    for (const [startX, startY] of startPoints) {
      if (!visited[startY * width + startX]) {
        this.floodFillProduct(data, mask, visited, startX, startY, width, height, tolerance);
      }
    }
    
    return mask;
  }

  /**
   * Memory-efficient flood fill for product backgrounds
   */
  private floodFillProduct(
    data: Buffer,
    mask: Uint8Array,
    visited: Uint8Array,
    startX: number,
    startY: number,
    width: number,
    height: number,
    tolerance: number
  ): void {
    // Use iterative approach to prevent stack overflow
    const stack: [number, number][] = [[startX, startY]];
    
    // Get reference color from starting point
    const refIndex = (startY * width + startX) * 4;
    const refR = data[refIndex];
    const refG = data[refIndex + 1];
    const refB = data[refIndex + 2];
    
    while (stack.length > 0) {
      const [x, y] = stack.pop()!;
      const index = y * width + x;
      
      if (x < 0 || x >= width || y < 0 || y >= height || visited[index]) {
        continue;
      }
      
      const i = index * 4;
      const r = data[i];
      const g = data[i + 1];
      const b = data[i + 2];
      
      // Check if pixel is similar to reference (background)
      const colorDistance = Math.sqrt(
        Math.pow(r - refR, 2) + 
        Math.pow(g - refG, 2) + 
        Math.pow(b - refB, 2)
      );
      
      if (colorDistance <= tolerance) {
        visited[index] = 1;
        mask[index] = 1;
        
        // Add neighbors (4-connectivity to save memory)
        stack.push([x + 1, y], [x - 1, y], [x, y + 1], [x, y - 1]);
      }
    }
  }

  /**
   * Calculate alpha value based on distance to product edge
   */
  private calculateEdgeAlpha(
    x: number, 
    y: number, 
    width: number, 
    height: number, 
    mask: Uint8Array, 
    smoothing: number
  ): number {
    const maxDistance = Math.ceil(smoothing * 3);
    
    // Find distance to nearest foreground pixel
    for (let dist = 1; dist <= maxDistance; dist++) {
      // Check pixels at distance 'dist' (diamond pattern for efficiency)
      const checkPoints: [number, number][] = [];
      
      for (let dx = -dist; dx <= dist; dx++) {
        const dy = dist - Math.abs(dx);
        checkPoints.push([x + dx, y + dy], [x + dx, y - dy]);
      }
      
      for (const [nx, ny] of checkPoints) {
        if (nx >= 0 && nx < width && ny >= 0 && ny < height) {
          if (mask[ny * width + nx] === 0) {
            // Found foreground pixel, calculate smooth alpha
            const normalizedDist = Math.min(1, dist / (smoothing * 2));
            return Math.round((1 - normalizedDist) * 255);
          }
        }
      }
    }
    
    return 0; // Fully transparent if no foreground found nearby
  }

  /**
   * Memory-efficient batch processing with progress tracking
   */
  async batchProcessProducts(
    imageUrls: string[],
    options: ProductOptions = {}
  ): Promise<{ successful: number; failed: number; results: ProcessingResult[]; totalMemoryUsed: string }> {
    const results: ProcessingResult[] = [];
    let successful = 0;
    let failed = 0;
    let totalMemoryUsed = 0;

    console.log(`🚀 Starting product batch processing of ${imageUrls.length} images...`);
    console.log(`📊 Max size: ${options.maxWidth || this.defaultOptions.maxWidth}x${options.maxHeight || this.defaultOptions.maxHeight}`);

    for (let i = 0; i < imageUrls.length; i++) {
      const url = imageUrls[i];
      const filename = `product_${String(i + 1).padStart(4, '0')}.webp`;
      
      console.log(`\n📦 Processing ${i + 1}/${imageUrls.length}: ${url}`);
      console.log(`💾 Current memory: ${Math.round(process.memoryUsage().heapUsed / 1024 / 1024)}MB`);

      const result = await this.removeProductBackground(url, filename, options);
      results.push(result);
      
      if (result.success) {
        successful++;
        if (result.memoryUsed) {
          totalMemoryUsed += parseInt(result.memoryUsed.replace('MB', ''));
        }
      } else {
        failed++;
      }

      // Force garbage collection between images if available
      if (global.gc && i % 5 === 4) {
        console.log(`🧹 Running garbage collection...`);
        global.gc();
      }
    }

    console.log(`\n✨ Product batch processing complete!`);
    console.log(`✅ Successful: ${successful}`);
    console.log(`❌ Failed: ${failed}`);
    console.log(`💾 Total memory used: ${totalMemoryUsed}MB`);

    return { successful, failed, results, totalMemoryUsed: `${totalMemoryUsed}MB` };
  }

  /**
   * Quick preview processing (lower quality, faster)
   */
  async quickPreview(
    imageUrl: string,
    outputFilename: string,
    options: ProductOptions = {}
  ): Promise<ProcessingResult> {
    const previewOptions = {
      ...options,
      maxWidth: 600,
      maxHeight: 600,
      quality: 70,
      compressionLevel: 4
    };
    
    console.log(`⚡ Quick preview mode for: ${imageUrl}`);
    return this.removeProductBackground(imageUrl, outputFilename, previewOptions);
  }
}

// Example usage optimized for Pokemon products
async function exampleUsage() {
  const productRemover = new ProductBackgroundRemover();

  // Single product processing
  await productRemover.removeProductBackground(
    'https://api.example.com/pokemon-booster-box.jpg',
    'booster_box_clean.webp',
    {
      maxWidth: 1000,           // Prevent memory issues
      maxHeight: 1000,
      backgroundTolerance: 30,  // Higher tolerance for varied backgrounds
      edgeSmoothing: 2,         // Smooth edges for product photography
      quality: 90
    }
  );

  // Batch processing with memory management
  const productUrls = [
    'https://api.example.com/booster-pack1.jpg',
    'https://api.example.com/elite-trainer-box.jpg',
    'https://api.example.com/tin-box.jpg'
  ];

  await productRemover.batchProcessProducts(productUrls, {
    maxWidth: 800,
    backgroundTolerance: 25,
    quality: 85,
    outputDir: 'pokemon_products_clean'
  });

  // Quick preview for testing
  await productRemover.quickPreview(
    'https://api.example.com/new-product.jpg',
    'preview.webp'
  );
}

// Run with garbage collection enabled: node --expose-gc your-script.js
export default ProductBackgroundRemover;