import 'dart:async';
import 'dart:io';
import 'package:flutter/foundation.dart'; // 用于 debugPrint

import 'package:PiliPlus/http/init.dart';
import 'package:PiliPlus/models_new/download/bili_download_entry_info.dart';
import 'package:PiliPlus/utils/extension/file_ext.dart';
import 'package:PiliPlus/utils/extension/string_ext.dart';
import 'package:dio/dio.dart';

class DownloadManager {
  final String url;
  final String path;
  final void Function(int, int)? onReceiveProgress;
  final void Function([Object? error]) onDone;

  // ⚠️ 优化：B站CDN对单IP并发有限制，3-4个线程通常是最优解，6个容易被限速或断开
  static const int _threadCount = 4;
  // ⚠️ 优化：降低分片阈值到 500KB，让更多文件能触发多线程
  static const int _minChunkSize = 500 * 1024; 

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
      debugPrint('🚀 [Download] 开始获取文件大小: $url');
      final headResp = await Request.http11Dio.head(
        url.http2https,
        options: Options(
          headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://www.bilibili.com/'
          },
          validateStatus: (status) => status != null && status < 500,
        ),
      );
      
      final contentLengthStr = headResp.headers.value(HttpHeaders.contentLengthHeader);
      if (contentLengthStr == null || contentLengthStr.isEmpty) {
        throw Exception("无法获取文件大小，CDN可能不支持HEAD请求");
      }
      
      final totalSize = int.parse(contentLengthStr);
      debugPrint('📦 [Download] 文件总大小: ${(totalSize / 1024 / 1024).toStringAsFixed(2)} MB');

      if (totalSize < _minChunkSize) {
        debugPrint('⚠️ [Download] 文件过小，使用单线程下载');
        await _downloadSingleThread(totalSize);
        return;
      }

      debugPrint('⚡ [Download] 文件大小达标，启动 $_threadCount 线程并发加速下载');
      await _downloadMultiThread(totalSize);

    } catch (e) {
      debugPrint('❌ [Download] 初始化失败: $e');
      _status = DownloadStatus.failDownload;
      onDone(e);
    }
  }

  // ... (中间的单线程 _downloadSingleThread 保持不变) ...
  Future<void> _downloadSingleThread(int totalSize) async {
    final file = File(path);
    if (!file.parent.existsSync()) file.parent.createSync(recursive: true);
    
    int received = 0;
    if (file.existsSync()) {
      received = await file.length();
      if (received >= totalSize) {
        _status = DownloadStatus.completed;
        onDone();
        return;
      }
    }

    final sink = file.openWrite(mode: received == 0 ? FileMode.writeOnly : FileMode.writeOnlyAppend);
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

  Future<void> _downloadMultiThread(int totalSize) async {
    final chunkSize = (totalSize / _threadCount).ceil();
    final tempDir = Directory(path).parent;
    final fileName = path.split('/').last;
    
    final partPaths = <String>[];
    for (int i = 0; i < _threadCount; i++) {
      partPaths.add('${tempDir.path}/.$fileName.part$i');
    }

    final receivedPerThread = List<int>.filled(_threadCount, 0);
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
          final currentTotal = receivedPerThread.reduce((a, b) => a + b);
          onReceiveProgress?.call(currentTotal, totalSize);
        },
      ));
    }

    try {
      debugPrint('⏳ [Download] 等待所有分片下载完成...');
      await Future.wait(futures);
      debugPrint('🔗 [Download] 分片下载完成，开始合并文件...');
      await _mergeParts(path, partPaths);
      debugPrint('✅ [Download] 合并完成！');
      _status = DownloadStatus.completed;
      onDone();
    } catch (e) {
      debugPrint('❌ [Download] 多线程下载失败，尝试清理临时文件: $e');
      for (final p in partPaths) {
        final f = File(p);
        if (f.existsSync()) await f.delete();
      }
      rethrow;
    }
  }

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
      // debugPrint('🧵 [Thread $index] 开始下载: bytes=$start-$end');
      final response = await Request.http11Dio.get<ResponseBody>(
        url.http2https,
        options: Options(
          headers: {
            'Range': 'bytes=$start-$end',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://www.bilibili.com/'
          },
          responseType: ResponseType.stream,
        ),
        cancelToken: _cancelToken,
      );

      // 检查是否真的返回了 206 Partial Content
      if (response.statusCode != 206) {
        throw Exception('Thread $index: 服务器不支持 Range 请求 (Status: ${response.statusCode})');
      }

      int receivedInChunk = 0;
      await for (final chunk in response.data!.stream) {
        if (_cancelToken.isCancelled) break;
        sink.add(chunk);
        receivedInChunk += chunk.length;
        onChunkReceived(receivedInChunk);
      }
      await sink.close();
      // debugPrint('✅ [Thread $index] 分片下载完成');
    } catch (e) {
      await sink.close();
      debugPrint('❌ [Thread $index] 下载失败: $e');
      rethrow; // 抛出错误，让 Future.wait 捕获并中止整个任务
    }
  }

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
        await partFile.delete();
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
    try { await task; } catch (_) {}
    
    if (isDelete) {
      final file = File(path);
      if (file.existsSync()) await file.tryDel();
      final dir = file.parent;
      final fileName = file.path.split('/').last;
      final parts = await dir.list().where((e) => e.path.contains('.$fileName.part')).toList();
      for (final p in parts) {
        await p.delete();
      }
    }
  }
}
