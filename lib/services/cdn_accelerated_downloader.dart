import 'dart:io';
import 'dart:async';
import 'package:dio/dio.dart';
import 'package:path_provider/path_provider.dart';

class CdnAcceleratedDownloader {
  final Dio _dio = Dio();
  
  // 配置：分片大小 (1MB)，并发数 (4)
  static const int chunkSize = 1024 * 1024; 
  static const int concurrency = 4;

  /// 主入口：下载视频并加速
  Future<void> downloadWithAcceleration({
    required String videoUrl,
    required String fileName,
    required Function(double progress) onProgress,
  }) async {
    final dir = await getApplicationDocumentsDirectory();
    final filePath = '${dir.path}/$fileName';
    
    // 1. 获取文件总大小
    final headResponse = await _dio.head(videoUrl);
    final totalSize = int.parse(headResponse.headers.value(HttpHeaders.contentLengthHeader) ?? '0');
    
    if (totalSize == 0) throw Exception('Cannot get file size');

    // 2. 创建空文件
    final file = File(filePath);
    await file.create();
    await file.writeAsBytes(List.filled(totalSize, 0)); // 预分配空间

    // 3. 计算分片范围
    final chunks = <Map<String, int>>[];
    for (int start = 0; start < totalSize; start += chunkSize) {
      final end = (start + chunkSize - 1 < totalSize) ? start + chunkSize - 1 : totalSize - 1;
      chunks.add({'start': start, 'end': end});
    }

    // 4. 并发下载分片 (使用 Future.wait 限制并发)
    final downloadedChunks = <int, List<int>>{};
    int completedChunks = 0;
    
    // 简单的并发控制
    for (var i = 0; i < chunks.length; i += concurrency) {
      final batch = chunks.skip(i).take(concurrency).toList();
      
      await Future.wait(batch.map((chunk) async {
        final start = chunk['start']!;
        final end = chunk['end']!;
        
        try {
          final response = await _dio.get<List<int>>(
            videoUrl,
            options: Options(
              headers: {
                HttpHeaders.rangeHeader: 'bytes=$start-$end',
                HttpHeaders.userAgentHeader: 'Mozilla/5.0...', // 模仿浏览器
              },
              responseType: ResponseType.bytes,
            ),
          );
          
          downloadedChunks[start] = response.data!;
          
          // 更新进度
          completedChunks++;
          final progress = (completedChunks / chunks.length) * 100;
          onProgress(progress);
          
        } catch (e) {
          print('Chunk download failed at $start: $e');
          // 这里可以添加重试逻辑
        }
      }));
    }

    // 5. 合并分片到文件
    final raf = await file.open(mode: FileMode.write);
    for (var i = 0; i < chunks.length; i++) {
      final start = chunks[i]['start']!;
      final data = downloadedChunks[start];
      if (data != null) {
        await raf.setPosition(start);
        await raf.writeFrom(data);
      }
    }
    await raf.close();
    
    print('Download completed: $filePath');
  }
}
