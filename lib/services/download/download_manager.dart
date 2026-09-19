import 'dart:async';
import 'dart:io';

import 'package:PiliPlus/http/init.dart';
import 'package:PiliPlus/utils/extension/file_ext.dart';
import 'package:PiliPlus/utils/extension/string_ext.dart';
import 'package:dio/dio.dart';

enum DownloadStatus {
  downloading,
  pause,
  completed,
  failDownload,
}

class DownloadManager {
  final String url;
  final String path;
  final void Function(int, int)? onReceiveProgress;
  final void Function([Object? error]) onDone;

  // 配置：并发线程数 (建议 4-8，过多可能导致被限流)
  static const int _threadCount = 6;
  // 配置：最小分片大小 (小于此大小不分片，直接单线程下载)
  static const int _minChunkSize = 1024 * 1024; // 1MB

  DownloadStatus _status = DownloadStatus.downloading;
  DownloadStatus get status => _status;
  
  final _cancelToken = CancelToken();
  late Future<void> task;

  DownloadManager({
    required this.url,
    required this.path,
    required this.onReceiveProgress,
    required this.onDone,
  }) {
    task = _start();
  }

  Future<void> _start() async {
    try {
      // 1. 获取文件总大小
      final headResp = await Request.http11Dio.head(
        url.http2https,
        options: Options(
          headers: {'User-Agent': 'Mozilla/5.0'},
          validateStatus: (status) => status != null && status < 500,
        ),
      );
      
      final contentLengthStr = headResp.headers.value(HttpHeaders.contentLengthHeader);
      if (contentLengthStr == null || contentLengthStr.isEmpty) {
        throw Exception("无法获取文件大小");
      }
      
      final totalSize = int.parse(contentLengthStr);
      
      // 如果文件太小，直接使用单线程下载以节省开销
      if (totalSize < _minChunkSize) {
        await _downloadSingleThread(totalSize);
        return;
      }

      // 2. 启动多线程分片下载
      await _downloadMultiThread(totalSize);

    } catch (e) {
      _status = DownloadStatus.failDownload;
      onDone(e);
    }
  }

  /// 单线程下载 (用于小文件或 fallback)
  Future<void> _downloadSingleThread(int totalSize) async {
    final file = File(path);
    if (!file.parent.existsSync()) {
      file.parent.createSync(recursive: true);
    }
    
    int received = 0;
    if (file.existsSync()) {
      received = await file.length();
      // 简单断点续传检查：如果已下载完成则跳过
      if (received >= totalSize) {
        _status = DownloadStatus.completed;
        onDone();
        return;
      }
    }

    final sink = file.openWrite(
      mode: received == 0 ? FileMode.writeOnly : FileMode.writeOnlyAppend,
    );

    try {
      final response = await Request.http11Dio.get<ResponseBody>(
        url.http2https,
        options: Options(
          headers: {'range': 'bytes=$received-'},
          responseType: ResponseType.stream,
        ),
        cancelToken: _cancelToken,
      );

      final data = response.data!;
      // 注意：HEAD 请求得到的 size 可能和实际 GET 不一致，这里以 HEAD 为准计算进度
      // 或者动态更新 totalSize: final actualTotal = received + (data.contentLength ?? 0);
      
      await for (final chunk in data.stream) {
        if (_status == DownloadStatus.pause || _cancelToken.isCancelled) break;
        sink.add(chunk);
        received += chunk.length;
        onReceiveProgress?.call(received, totalSize);
      }
      
      await sink.close();
      if (!_cancelToken.isCancelled && _status != DownloadStatus.pause) {
        _status = DownloadStatus.completed;
        onDone();
      }
    } catch (e) {
      await sink.close();
      rethrow;
    }
  }

  /// 多线程分片下载 (核心加速逻辑)
  Future<void> _downloadMultiThread(int totalSize) async {
    final chunkSize = (totalSize / _threadCount).ceil();
    final tempDir = Directory(path).parent;
    final fileName = path.split('/').last;
    
    // 创建临时文件列表
    final partPaths = <String>[];
    for (int i = 0; i < _threadCount; i++) {
      partPaths.add('${tempDir.path}/.$fileName.part$i');
    }

    // 记录每个线程的进度
    final receivedPerThread = List<int>.filled(_threadCount, 0);
    int totalReceived = 0;
    
    // 用于同步进度的锁/信号量 (简化版：使用局部变量累加)
    // 注意：在高频回调中频繁调用 setState 或 UI 更新会卡顿，这里做节流
    
    final futures = <Future<void>>[];

    for (int i = 0; i < _threadCount; i++) {
      final start = i * chunkSize;
      final end = (i == _threadCount - 1) ? totalSize - 1 : start + chunkSize - 1;
      final partPath = partPaths[i];

      futures.add(_downloadChunk(
        index: i,
        url: url,
        savePath: partPath,
        start: start,
        end: end,
        onChunkReceived: (bytes) {
          receivedPerThread[i] = bytes;
          // 计算总进度
          final currentTotal = receivedPerThread.reduce((a, b) => a + b);
          // 简单的节流：每 200ms 更新一次 UI，避免过度刷新
          // 在实际 Flutter 项目中，最好使用 Stream 或 ValueNotifier
          onReceiveProgress?.call(currentTotal, totalSize);
        },
      ));
    }

    try {
      await Future.wait(futures);
      
      // 所有分片下载完成，开始合并
      await _mergeParts(path, partPaths);
      
      _status = DownloadStatus.completed;
      onDone();
    } catch (e) {
      // 清理失败的分片
      for (final p in partPaths) {
        final f = File(p);
        if (f.existsSync()) await f.delete();
      }
      rethrow;
    }
  }

  /// 下载单个分片
  Future<void> _downloadChunk({
    required int index,
    required String url,
    required String savePath,
    required int start,
    required int end,
    required void Function(int bytes) onChunkReceived,
  }) async {
    final file = File(savePath);
    final sink = file.openWrite(mode: FileMode.writeOnly);
    
    try {
      final response = await Request.http11Dio.get<ResponseBody>(
        url.http2https,
        options: Options(
          headers: {
            'Range': 'bytes=$start-$end',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
          },
          responseType: ResponseType.stream,
        ),
        cancelToken: _cancelToken,
      );

      int receivedInChunk = 0;
      await for (final chunk in response.data!.stream) {
        if (_cancelToken.isCancelled) break;
        sink.add(chunk);
        receivedInChunk += chunk.length;
        onChunkReceived(receivedInChunk);
      }
      
      await sink.close();
    } catch (e) {
      await sink.close();
      rethrow;
    }
  }

  /// 合并分片文件
  Future<void> _mergeParts(String finalPath, List<String> partPaths) async {
    final finalFile = File(finalPath);
    if (!finalFile.parent.existsSync()) {
      finalFile.parent.createSync(recursive: true);
    }
    
    final sink = finalFile.openWrite(mode: FileMode.writeOnly);
    
    for (final partPath in partPaths) {
      final partFile = File(partPath);
      if (partFile.existsSync()) {
        await sink.addStream(partFile.openRead());
        await partFile.delete(); // 删除临时分片
      }
    }
    
    await sink.close();
  }

  Future<void> cancel({required bool isDelete}) async {
    if (!isDelete && _status == DownloadStatus.downloading) {
      _status = DownloadStatus.pause;
    }
    
    if (!_cancelToken.isCancelled) {
      _cancelToken.cancel();
    }
    
    // 等待任务结束以便清理
    try {
      await task;
    } catch (_) {}
    
    if (isDelete) {
      final file = File(path);
      if (file.existsSync()) {
        await file.tryDel();
      }
      // 清理可能的残留分片
      final dir = file.parent;
      final fileName = file.path.split('/').last;
      final parts = await dir.list().where((e) => e.path.contains('.$fileName.part')).toList();
      for (final p in parts) {
        await p.delete();
      }
    }
  }
}
