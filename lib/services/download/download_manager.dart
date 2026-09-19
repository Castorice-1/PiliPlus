import 'dart:async';
import 'dart:io';

import 'package:PiliPlus/http/init.dart';
import 'package:PiliPlus/models_new/download/bili_download_entry_info.dart'; // 导入原有的 DownloadStatus
import 'package:PiliPlus/utils/extension/file_ext.dart';
import 'package:PiliPlus/utils/extension/string_ext.dart';
import 'package:dio/dio.dart';

class DownloadManager {
  final String url;
  final String path;
  final void Function(int, int)? onReceiveProgress;
  final void Function([Object? error]) onDone;

  // 配置：并发线程数 (建议 4-8)
  static const int _threadCount = 6;
  // 配置：最小分片大小 (小于此大小不分片，直接单线程下载，单位: 字节)
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
    
    final partPaths = <String>[];
    for (int i = 0; i < _threadCount; i++) {
