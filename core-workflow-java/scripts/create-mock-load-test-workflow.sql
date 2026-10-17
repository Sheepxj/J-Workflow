SET @flow_id = 826202600000000001;
SET @flow_version = 'mock-load-test-v1';
SET @start_id = 'node-start::mock-load-test';
SET @end_id = 'node-end::mock-load-test';

SET @nodes = JSON_ARRAY(
    JSON_OBJECT(
        'id', @start_id,
        'data', JSON_OBJECT(
            'inputs', JSON_ARRAY(),
            'nodeMeta', JSON_OBJECT('aliasName', '开始', 'nodeType', '基础节点'),
            'nodeParam', JSON_OBJECT(),
            'outputs', JSON_ARRAY(JSON_OBJECT('name', 'AGENT_USER_INPUT', 'schema', JSON_OBJECT('type', 'string')))
        )
    ),
    JSON_OBJECT(
        'id', 'mock::01',
        'data', JSON_OBJECT(
            'inputs', JSON_ARRAY(JSON_OBJECT('name', 'input', 'schema', JSON_OBJECT('type', 'string', 'value', JSON_OBJECT('type', 'ref', 'content', JSON_OBJECT('nodeId', @start_id, 'name', 'AGENT_USER_INPUT'))))),
            'nodeMeta', JSON_OBJECT('aliasName', 'Mock 01', 'nodeType', 'Mock'),
            'nodeParam', JSON_OBJECT('delayMs', 100, 'jitterMs', 0, 'failureRate', 0, 'payloadBytes', 0),
            'outputs', JSON_ARRAY(JSON_OBJECT('name', 'output', 'schema', JSON_OBJECT('type', 'string')))
        )
    ),
    JSON_OBJECT(
        'id', 'mock::02',
        'data', JSON_OBJECT(
            'inputs', JSON_ARRAY(JSON_OBJECT('name', 'input', 'schema', JSON_OBJECT('type', 'string', 'value', JSON_OBJECT('type', 'ref', 'content', JSON_OBJECT('nodeId', 'mock::01', 'name', 'output'))))),
            'nodeMeta', JSON_OBJECT('aliasName', 'Mock 02', 'nodeType', 'Mock'),
            'nodeParam', JSON_OBJECT('delayMs', 100, 'jitterMs', 0, 'failureRate', 0, 'payloadBytes', 0),
            'outputs', JSON_ARRAY(JSON_OBJECT('name', 'output', 'schema', JSON_OBJECT('type', 'string')))
        )
    ),
    JSON_OBJECT(
        'id', 'mock::03',
        'data', JSON_OBJECT(
            'inputs', JSON_ARRAY(JSON_OBJECT('name', 'input', 'schema', JSON_OBJECT('type', 'string', 'value', JSON_OBJECT('type', 'ref', 'content', JSON_OBJECT('nodeId', 'mock::02', 'name', 'output'))))),
            'nodeMeta', JSON_OBJECT('aliasName', 'Mock 03', 'nodeType', 'Mock'),
            'nodeParam', JSON_OBJECT('delayMs', 100, 'jitterMs', 0, 'failureRate', 0, 'payloadBytes', 0),
            'outputs', JSON_ARRAY(JSON_OBJECT('name', 'output', 'schema', JSON_OBJECT('type', 'string')))
        )
    ),
    JSON_OBJECT(
        'id', 'mock::04',
        'data', JSON_OBJECT(
            'inputs', JSON_ARRAY(JSON_OBJECT('name', 'input', 'schema', JSON_OBJECT('type', 'string', 'value', JSON_OBJECT('type', 'ref', 'content', JSON_OBJECT('nodeId', 'mock::03', 'name', 'output'))))),
            'nodeMeta', JSON_OBJECT('aliasName', 'Mock 04', 'nodeType', 'Mock'),
            'nodeParam', JSON_OBJECT('delayMs', 100, 'jitterMs', 0, 'failureRate', 0, 'payloadBytes', 0),
            'outputs', JSON_ARRAY(JSON_OBJECT('name', 'output', 'schema', JSON_OBJECT('type', 'string')))
        )
    ),
    JSON_OBJECT(
        'id', 'mock::05',
        'data', JSON_OBJECT(
            'inputs', JSON_ARRAY(JSON_OBJECT('name', 'input', 'schema', JSON_OBJECT('type', 'string', 'value', JSON_OBJECT('type', 'ref', 'content', JSON_OBJECT('nodeId', 'mock::04', 'name', 'output'))))),
            'nodeMeta', JSON_OBJECT('aliasName', 'Mock 05', 'nodeType', 'Mock'),
            'nodeParam', JSON_OBJECT('delayMs', 100, 'jitterMs', 0, 'failureRate', 0, 'payloadBytes', 0),
            'outputs', JSON_ARRAY(JSON_OBJECT('name', 'output', 'schema', JSON_OBJECT('type', 'string')))
        )
    ),
    JSON_OBJECT(
        'id', 'mock::06',
        'data', JSON_OBJECT(
            'inputs', JSON_ARRAY(JSON_OBJECT('name', 'input', 'schema', JSON_OBJECT('type', 'string', 'value', JSON_OBJECT('type', 'ref', 'content', JSON_OBJECT('nodeId', 'mock::05', 'name', 'output'))))),
            'nodeMeta', JSON_OBJECT('aliasName', 'Mock 06', 'nodeType', 'Mock'),
            'nodeParam', JSON_OBJECT('delayMs', 100, 'jitterMs', 0, 'failureRate', 0, 'payloadBytes', 0),
            'outputs', JSON_ARRAY(JSON_OBJECT('name', 'output', 'schema', JSON_OBJECT('type', 'string')))
        )
    ),
    JSON_OBJECT(
        'id', 'mock::07',
        'data', JSON_OBJECT(
            'inputs', JSON_ARRAY(JSON_OBJECT('name', 'input', 'schema', JSON_OBJECT('type', 'string', 'value', JSON_OBJECT('type', 'ref', 'content', JSON_OBJECT('nodeId', 'mock::06', 'name', 'output'))))),
            'nodeMeta', JSON_OBJECT('aliasName', 'Mock 07', 'nodeType', 'Mock'),
            'nodeParam', JSON_OBJECT('delayMs', 100, 'jitterMs', 0, 'failureRate', 0, 'payloadBytes', 0),
            'outputs', JSON_ARRAY(JSON_OBJECT('name', 'output', 'schema', JSON_OBJECT('type', 'string')))
        )
    ),
    JSON_OBJECT(
        'id', 'mock::08',
        'data', JSON_OBJECT(
            'inputs', JSON_ARRAY(JSON_OBJECT('name', 'input', 'schema', JSON_OBJECT('type', 'string', 'value', JSON_OBJECT('type', 'ref', 'content', JSON_OBJECT('nodeId', 'mock::07', 'name', 'output'))))),
            'nodeMeta', JSON_OBJECT('aliasName', 'Mock 08', 'nodeType', 'Mock'),
            'nodeParam', JSON_OBJECT('delayMs', 100, 'jitterMs', 0, 'failureRate', 0, 'payloadBytes', 0),
            'outputs', JSON_ARRAY(JSON_OBJECT('name', 'output', 'schema', JSON_OBJECT('type', 'string')))
        )
    ),
    JSON_OBJECT(
        'id', 'mock::09',
        'data', JSON_OBJECT(
            'inputs', JSON_ARRAY(JSON_OBJECT('name', 'input', 'schema', JSON_OBJECT('type', 'string', 'value', JSON_OBJECT('type', 'ref', 'content', JSON_OBJECT('nodeId', 'mock::08', 'name', 'output'))))),
            'nodeMeta', JSON_OBJECT('aliasName', 'Mock 09', 'nodeType', 'Mock'),
            'nodeParam', JSON_OBJECT('delayMs', 100, 'jitterMs', 0, 'failureRate', 0, 'payloadBytes', 0),
            'outputs', JSON_ARRAY(JSON_OBJECT('name', 'output', 'schema', JSON_OBJECT('type', 'string')))
        )
    ),
    JSON_OBJECT(
        'id', 'mock::10',
        'data', JSON_OBJECT(
            'inputs', JSON_ARRAY(JSON_OBJECT('name', 'input', 'schema', JSON_OBJECT('type', 'string', 'value', JSON_OBJECT('type', 'ref', 'content', JSON_OBJECT('nodeId', 'mock::09', 'name', 'output'))))),
            'nodeMeta', JSON_OBJECT('aliasName', 'Mock 10', 'nodeType', 'Mock'),
            'nodeParam', JSON_OBJECT('delayMs', 100, 'jitterMs', 0, 'failureRate', 0, 'payloadBytes', 0),
            'outputs', JSON_ARRAY(JSON_OBJECT('name', 'output', 'schema', JSON_OBJECT('type', 'string')))
        )
    ),
    JSON_OBJECT(
        'id', @end_id,
        'data', JSON_OBJECT(
            'inputs', JSON_ARRAY(JSON_OBJECT('name', 'input', 'schema', JSON_OBJECT('type', 'string', 'value', JSON_OBJECT('type', 'ref', 'content', JSON_OBJECT('nodeId', 'mock::10', 'name', 'output'))))),
            'nodeMeta', JSON_OBJECT('aliasName', '结束', 'nodeType', '基础节点'),
            'nodeParam', JSON_OBJECT('outputMode', 1, 'template', '{{input}}'),
            'outputs', JSON_ARRAY()
        )
    )
);

SET @edges = JSON_ARRAY(
    JSON_OBJECT('sourceNodeId', @start_id, 'targetNodeId', 'mock::01'),
    JSON_OBJECT('sourceNodeId', 'mock::01', 'targetNodeId', 'mock::02'),
    JSON_OBJECT('sourceNodeId', 'mock::02', 'targetNodeId', 'mock::03'),
    JSON_OBJECT('sourceNodeId', 'mock::03', 'targetNodeId', 'mock::04'),
    JSON_OBJECT('sourceNodeId', 'mock::04', 'targetNodeId', 'mock::05'),
    JSON_OBJECT('sourceNodeId', 'mock::05', 'targetNodeId', 'mock::06'),
    JSON_OBJECT('sourceNodeId', 'mock::06', 'targetNodeId', 'mock::07'),
    JSON_OBJECT('sourceNodeId', 'mock::07', 'targetNodeId', 'mock::08'),
    JSON_OBJECT('sourceNodeId', 'mock::08', 'targetNodeId', 'mock::09'),
    JSON_OBJECT('sourceNodeId', 'mock::09', 'targetNodeId', 'mock::10'),
    JSON_OBJECT('sourceNodeId', 'mock::10', 'targetNodeId', @end_id)
);

SET @flow_data = JSON_OBJECT(
    'data',
    JSON_OBJECT('nodes', CAST(@nodes AS JSON), 'edges', CAST(@edges AS JSON))
);

INSERT INTO flow (
    id, group_id, name, data, release_data, description, version, release_status,
    app_id, source, tag, create_by, update_by, create_at, update_at
) VALUES (
    @flow_id, @flow_id, 'Java Mock 节点串行压测', @flow_data, @flow_data,
    'START -> 10 x Mock(100ms) -> END，理论节点耗时约 1 秒', @flow_version, 1,
    'mock-load-test', 1, 0, 0, 0, NOW(), NOW()
)
ON DUPLICATE KEY UPDATE
    name = VALUES(name),
    data = VALUES(data),
    release_data = VALUES(release_data),
    description = VALUES(description),
    release_status = VALUES(release_status),
    app_id = VALUES(app_id),
    update_at = NOW();

SELECT id, name, description, CHAR_LENGTH(data) AS data_length
FROM flow
WHERE id = @flow_id;
