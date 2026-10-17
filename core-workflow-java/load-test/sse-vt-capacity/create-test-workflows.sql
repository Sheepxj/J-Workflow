SET @base_flow_id = 826202600000000001;

INSERT INTO flow
SELECT 826202600000000002, 826202600000000002, 'Java Mock 长连接压测',
       JSON_SET(data,
           '$.data.nodes[1].data.nodeParam.delayMs', 1000,
           '$.data.nodes[2].data.nodeParam.delayMs', 1000,
           '$.data.nodes[3].data.nodeParam.delayMs', 1000,
           '$.data.nodes[4].data.nodeParam.delayMs', 1000,
           '$.data.nodes[5].data.nodeParam.delayMs', 1000,
           '$.data.nodes[6].data.nodeParam.delayMs', 1000,
           '$.data.nodes[7].data.nodeParam.delayMs', 1000,
           '$.data.nodes[8].data.nodeParam.delayMs', 1000,
           '$.data.nodes[9].data.nodeParam.delayMs', 1000,
           '$.data.nodes[10].data.nodeParam.delayMs', 1000),
       JSON_SET(release_data,
           '$.data.nodes[1].data.nodeParam.delayMs', 1000,
           '$.data.nodes[2].data.nodeParam.delayMs', 1000,
           '$.data.nodes[3].data.nodeParam.delayMs', 1000,
           '$.data.nodes[4].data.nodeParam.delayMs', 1000,
           '$.data.nodes[5].data.nodeParam.delayMs', 1000,
           '$.data.nodes[6].data.nodeParam.delayMs', 1000,
           '$.data.nodes[7].data.nodeParam.delayMs', 1000,
           '$.data.nodes[8].data.nodeParam.delayMs', 1000,
           '$.data.nodes[9].data.nodeParam.delayMs', 1000,
           '$.data.nodes[10].data.nodeParam.delayMs', 1000),
       'START -> 10 x Mock(1000ms) -> END，理论耗时约 10 秒', 'mock-long-v1', release_status,
       'mock-long-load-test', source, tag, create_by, update_by, NOW(), NOW()
FROM flow WHERE id = @base_flow_id
ON DUPLICATE KEY UPDATE data=VALUES(data), release_data=VALUES(release_data), description=VALUES(description), update_at=NOW();

INSERT INTO flow
SELECT 826202600000000003, 826202600000000003, 'Java Mock 高频小消息压测',
       JSON_SET(data,
           '$.data.nodes[1].data.nodeParam.delayMs', 0,
           '$.data.nodes[1].data.nodeParam.eventCount', 2000,
           '$.data.nodes[1].data.nodeParam.eventBytes', 128),
       JSON_SET(release_data,
           '$.data.nodes[1].data.nodeParam.delayMs', 0,
           '$.data.nodes[1].data.nodeParam.eventCount', 2000,
           '$.data.nodes[1].data.nodeParam.eventBytes', 128),
       '首个 Mock 连续发送 2000 条 128 字节事件', 'mock-burst-v1', release_status,
       'mock-burst-load-test', source, tag, create_by, update_by, NOW(), NOW()
FROM flow WHERE id = @base_flow_id
ON DUPLICATE KEY UPDATE data=VALUES(data), release_data=VALUES(release_data), description=VALUES(description), update_at=NOW();

INSERT INTO flow
SELECT 826202600000000004, 826202600000000004, 'Java Mock SSE 背压压测',
       JSON_SET(data,
           '$.data.nodes[1].data.nodeParam.delayMs', 0,
           '$.data.nodes[1].data.nodeParam.eventCount', 5000,
           '$.data.nodes[1].data.nodeParam.eventBytes', 4096),
       JSON_SET(release_data,
           '$.data.nodes[1].data.nodeParam.delayMs', 0,
           '$.data.nodes[1].data.nodeParam.eventCount', 5000,
           '$.data.nodes[1].data.nodeParam.eventBytes', 4096),
       '首个 Mock 连续发送 5000 条 4096 字节事件，用于慢客户端和 1024 队列背压', 'mock-backpressure-v1', release_status,
       'mock-backpressure-load-test', source, tag, create_by, update_by, NOW(), NOW()
FROM flow WHERE id = @base_flow_id
ON DUPLICATE KEY UPDATE data=VALUES(data), release_data=VALUES(release_data), description=VALUES(description), update_at=NOW();

INSERT INTO flow
SELECT 826202600000000005, 826202600000000005, 'Java Mock 执行异常压测',
       JSON_SET(data, '$.data.nodes[1].data.nodeParam.failureRate', 1),
       JSON_SET(release_data, '$.data.nodes[1].data.nodeParam.failureRate', 1),
       '首个 Mock 节点固定失败', 'mock-failure-v1', release_status,
       'mock-failure-load-test', source, tag, create_by, update_by, NOW(), NOW()
FROM flow WHERE id = @base_flow_id
ON DUPLICATE KEY UPDATE data=VALUES(data), release_data=VALUES(release_data), description=VALUES(description), update_at=NOW();

SELECT id, name, description FROM flow WHERE id BETWEEN 826202600000000001 AND 826202600000000005 ORDER BY id;
