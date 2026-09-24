"""Recursive, type-aware comparison of AiiDA nodes.

Process ports and subprocesses are aligned by link label so diff paths mirror
the namespaces and call hierarchy visible in AiiDA.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime
from fnmatch import fnmatchcase
import math
from numbers import Real
from pathlib import PurePath
from typing import Any, Callable, Iterable

from aiida import orm
from aiida.common.links import LinkType

_MISSING = object()

_CATEGORY_ORDER = (
    'inputs',
    'outputs',
    'output_files',
    'process',
    'attributes',
    'extras',
    'metadata',
    'files',
    'runtime',
)
_CATEGORY_LABELS = {
    'inputs': 'Inputs',
    'outputs': 'Outputs',
    'output_files': 'Output files',
    'process': 'Process / code',
    'attributes': 'Attributes',
    'extras': 'Extras',
    'metadata': 'Metadata',
    'files': 'Repository files',
    'runtime': 'Runtime records',
}
_RUNTIME_PATH_PARTS = (
    '.attributes.detailed_job_info',
    '.attributes.last_job_info',
    '.attributes.job_id',
    '.attributes.scheduler_',
    '.attributes.remote_workdir',
    '.attributes.remote_path',
    '.attributes.wall_time_seconds',
)


@dataclass(frozen=True)
class NodeReference:
    """Small, serialisable reference to a node participating in a diff."""

    pk: int | None
    uuid: str | None
    node_type: str
    process_type: str | None = None
    process_label: str | None = None

    def __str__(self) -> str:
        label = self.process_label or self.node_type.rstrip('.').rsplit('.', 1)[-1]
        identifier = self.pk if self.pk is not None else self.uuid
        return f'{label}<{identifier}>'


@dataclass(frozen=True)
class DiffEntry:
    """One leaf difference between two nodes."""

    path: str
    kind: str
    left: Any = None
    right: Any = None
    detail: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            'path': self.path,
            'kind': self.kind,
            'left': _serialise_value(self.left),
            'right': _serialise_value(self.right),
            'detail': self.detail,
        }


@dataclass
class NodeDiff:
    """Structured result returned by compare_nodes."""

    left: NodeReference
    right: NodeReference
    entries: list[DiffEntry]
    compared_node_pairs: int
    truncated: bool = False

    @property
    def equal(self) -> bool:
        return not self.entries and not self.truncated

    def to_dict(self) -> dict[str, Any]:
        return {
            'left': asdict(self.left),
            'right': asdict(self.right),
            'equal': self.equal,
            'compared_node_pairs': self.compared_node_pairs,
            'truncated': self.truncated,
            'entries': [
                {**entry.to_dict(), 'category': _entry_category(entry)}
                for entry in self.entries
            ],
        }

    def grouped_entries(self) -> dict[str, list[DiffEntry]]:
        """Return differences grouped by their user-facing category."""
        grouped = {category: [] for category in _CATEGORY_ORDER}
        for entry in self.entries:
            grouped[_entry_category(entry)].append(entry)
        return grouped

    def format(
        self,
        max_entries: int | None = 100,
        *,
        show_runtime: bool = False,
        show_files: bool = False,
    ) -> str:
        """Render a categorized summary, hiding volatile details by default."""
        if self.equal:
            return f'{self.left} and {self.right}: no differences'

        grouped = self.grouped_entries()
        lines = [
            f'{self.left} vs {self.right}',
            f'{len(self.entries)} difference(s) across '
            f'{self.compared_node_pairs} compared node pair(s)',
            '',
            'Summary',
        ]
        for category in _CATEGORY_ORDER:
            count = len(grouped[category])
            suffix = ''
            if count and category == 'runtime' and not show_runtime:
                suffix = ' (details hidden)'
            elif count and category in ('files', 'output_files') and not show_files:
                suffix = ' (details hidden)'
            lines.append(f'  {_CATEGORY_LABELS[category]:<18} {count:>4}{suffix}')

        visible_categories = [
            category for category in _CATEGORY_ORDER
            if grouped[category]
            and (category != 'runtime' or show_runtime)
            and (category not in ('files', 'output_files') or show_files)
        ]
        remaining = max_entries
        for category in visible_categories:
            entries = grouped[category]
            if remaining is not None and remaining <= 0:
                break
            shown = entries if remaining is None else entries[:remaining]
            lines.extend(['', f'{_CATEGORY_LABELS[category]} ({len(entries)})'])
            lines.extend(_format_grouped_entries(shown))
            hidden = len(entries) - len(shown)
            if hidden:
                lines.append(f'  ... {hidden} more in this category')
            if remaining is not None:
                remaining -= len(shown)

        hidden_files = len(grouped['files']) + len(grouped['output_files'])
        if hidden_files and not show_files:
            lines.extend([
                '',
                f'File differences hidden: {hidden_files} '
                f'({len(grouped["output_files"])} output, '
                f'{len(grouped["files"])} other; use show_files=True)',
            ])
        if grouped['runtime'] and not show_runtime:
            lines.extend([
                '',
                f'Runtime differences hidden: {len(grouped["runtime"])} '
                '(use show_runtime=True)',
            ])
        if self.truncated:
            lines.append('... comparison stopped at max_differences')
        return '\n'.join(lines)

    def format_flat(self, max_entries: int | None = 100) -> str:
        """Render the original ungrouped, exhaustive diff."""
        if self.equal:
            return f'{self.left} and {self.right}: no differences'

        total = len(self.entries)
        shown = self.entries if max_entries is None else self.entries[:max_entries]
        lines = [
            f'{self.left} vs {self.right}: {total} difference(s) '
            f'across {self.compared_node_pairs} compared node pair(s)'
        ]
        lines.extend(_format_entry(entry) for entry in shown)
        hidden = total - len(shown)
        if hidden:
            lines.append(f'... {hidden} more difference(s) not shown')
        if self.truncated:
            lines.append('... comparison stopped at max_differences')
        return '\n'.join(lines)

    def __str__(self) -> str:
        return self.format()


@dataclass(frozen=True)
class CompareOptions:
    """Options controlling recursive node comparison."""

    compare_attributes: bool = True
    compare_extras: bool = True
    compare_repository: bool = True
    compare_inputs: bool = True
    compare_outputs: bool = True
    compare_subprocesses: bool = True
    compare_arrays: bool = True
    include_identity: bool = False
    include_internal_extras: bool = False
    max_depth: int | None = None
    max_differences: int | None = None
    rtol: float = 0.0
    atol: float = 0.0
    ignore_paths: tuple[str, ...] = ()


class _ComparisonContext:
    def __init__(self, options: CompareOptions):
        self.options = options
        self.entries: list[DiffEntry] = []
        self.seen_pairs: set[tuple[str, str]] = set()
        self.compared_node_pairs = 0
        self.truncated = False

    @property
    def full(self) -> bool:
        maximum = self.options.max_differences
        return maximum is not None and len(self.entries) >= maximum

    def ignored(self, path: str) -> bool:
        return any(fnmatchcase(path, pattern) for pattern in self.options.ignore_paths)

    def add(
        self,
        path: str,
        kind: str,
        left: Any = None,
        right: Any = None,
        detail: str | None = None,
    ) -> None:
        if self.ignored(path):
            return
        if self.full:
            self.truncated = True
            return
        self.entries.append(DiffEntry(path, kind, left, right, detail))


def compare_nodes(
    left: orm.Node | int | str,
    right: orm.Node | int | str,
    *,
    compare_attributes: bool = True,
    compare_extras: bool = True,
    compare_repository: bool = True,
    compare_inputs: bool = True,
    compare_outputs: bool = True,
    compare_subprocesses: bool = True,
    compare_arrays: bool = True,
    include_identity: bool = False,
    include_internal_extras: bool = False,
    max_depth: int | None = None,
    max_differences: int | None = None,
    rtol: float = 0.0,
    atol: float = 0.0,
    ignore_paths: Iterable[str] = (),
) -> NodeDiff:
    """Compare two AiiDA nodes and return all discovered leaf differences.

    max_depth limits call-link traversal only. Zero compares the root process
    and its data ports; None follows the complete call tree. Data nodes are not
    followed through creator links, keeping the traversal finite.

    Database identity and internal AiiDA extras are ignored by default because
    independently created nodes necessarily differ there.
    """
    if max_depth is not None and max_depth < 0:
        raise ValueError('max_depth must be non-negative or None')
    if max_differences is not None and max_differences <= 0:
        raise ValueError('max_differences must be positive or None')
    if rtol < 0 or atol < 0:
        raise ValueError('rtol and atol must be non-negative')

    left_node = _load_node(left)
    right_node = _load_node(right)
    options = CompareOptions(
        compare_attributes=compare_attributes,
        compare_extras=compare_extras,
        compare_repository=compare_repository,
        compare_inputs=compare_inputs,
        compare_outputs=compare_outputs,
        compare_subprocesses=compare_subprocesses,
        compare_arrays=compare_arrays,
        include_identity=include_identity,
        include_internal_extras=include_internal_extras,
        max_depth=max_depth,
        max_differences=max_differences,
        rtol=rtol,
        atol=atol,
        ignore_paths=tuple(ignore_paths),
    )
    context = _ComparisonContext(options)
    _compare_node(left_node, right_node, '$', 0, context)
    return NodeDiff(
        left=_node_reference(left_node),
        right=_node_reference(right_node),
        entries=context.entries,
        compared_node_pairs=context.compared_node_pairs,
        truncated=context.truncated,
    )


def compare(left: orm.Node | int | str, right: orm.Node | int | str, **kwargs: Any) -> NodeDiff:
    """Alias for compare_nodes."""
    return compare_nodes(left, right, **kwargs)


def _load_node(value: orm.Node | int | str) -> orm.Node:
    if isinstance(value, orm.Node) or (hasattr(value, 'base') and hasattr(value, 'node_type')):
        return value
    return orm.load_node(value)


def _node_reference(node: orm.Node) -> NodeReference:
    return NodeReference(
        pk=getattr(node, 'pk', None),
        uuid=str(getattr(node, 'uuid', '')) or None,
        node_type=str(getattr(node, 'node_type', node.__class__.__name__)),
        process_type=getattr(node, 'process_type', None),
        process_label=getattr(node, 'process_label', None),
    )


def _pair_key(left: orm.Node, right: orm.Node) -> tuple[str, str]:
    def identifier(node: orm.Node) -> str:
        uuid = getattr(node, 'uuid', None)
        return str(uuid) if uuid is not None else f'object:{id(node)}'

    return identifier(left), identifier(right)


def _is_process_node(node: orm.Node) -> bool:
    return isinstance(node, orm.ProcessNode) or str(getattr(node, 'node_type', '')).startswith('process.')


def _compare_node(
    left: orm.Node,
    right: orm.Node,
    path: str,
    process_depth: int,
    context: _ComparisonContext,
) -> None:
    if context.full:
        context.truncated = True
        return

    pair = _pair_key(left, right)
    if pair in context.seen_pairs:
        return
    context.seen_pairs.add(pair)
    context.compared_node_pairs += 1

    _compare_metadata(left, right, _join(path, 'metadata'), context)
    if context.options.compare_attributes:
        _compare_section(
            lambda: dict(left.base.attributes.all),
            lambda: dict(right.base.attributes.all),
            _join(path, 'attributes'),
            context,
        )
    if context.options.compare_extras:
        _compare_section(
            lambda: _extras(left, context.options.include_internal_extras),
            lambda: _extras(right, context.options.include_internal_extras),
            _join(path, 'extras'),
            context,
        )
    if context.options.compare_repository:
        _compare_repository(left, right, _join(path, 'repository'), context)
    if context.options.compare_arrays and isinstance(left, orm.ArrayData) and isinstance(right, orm.ArrayData):
        _compare_array_data(left, right, _join(path, 'arrays'), context)

    if not (_is_process_node(left) and _is_process_node(right)):
        return
    if context.options.compare_inputs:
        _compare_links(
            left, right,
            direction='incoming',
            link_types=(LinkType.INPUT_CALC, LinkType.INPUT_WORK),
            path=_join(path, 'inputs'),
            child_depth=process_depth,
            context=context,
        )
    within_depth = context.options.max_depth is None or process_depth < context.options.max_depth
    if context.options.compare_subprocesses and within_depth:
        _compare_links(
            left, right,
            direction='outgoing',
            link_types=(LinkType.CALL_CALC, LinkType.CALL_WORK),
            path=_join(path, 'called'),
            child_depth=process_depth + 1,
            context=context,
        )
    if context.options.compare_outputs:
        _compare_links(
            left, right,
            direction='outgoing',
            link_types=(LinkType.CREATE, LinkType.RETURN),
            path=_join(path, 'outputs'),
            child_depth=process_depth,
            context=context,
        )


def _extras(node: orm.Node, include_internal: bool) -> dict[str, Any]:
    values = dict(node.base.extras.all)
    if include_internal:
        return values
    return {key: value for key, value in values.items() if not key.startswith('_aiida_')}

def _compare_metadata(left: orm.Node, right: orm.Node, path: str, context: _ComparisonContext) -> None:
    fields: dict[str, Callable[[orm.Node], Any]] = {
        'node_type': lambda node: getattr(node, 'node_type', None),
        'process_type': lambda node: getattr(node, 'process_type', None),
        'label': lambda node: getattr(node, 'label', None),
        'description': lambda node: getattr(node, 'description', None),
        'computer': _computer_reference,
        'user': lambda node: getattr(getattr(node, 'user', None), 'email', None),
    }
    if context.options.include_identity:
        fields.update({
            'pk': lambda node: getattr(node, 'pk', None),
            'uuid': lambda node: str(getattr(node, 'uuid', None)),
            'ctime': lambda node: getattr(node, 'ctime', None),
            'mtime': lambda node: getattr(node, 'mtime', None),
        })
    for name, getter in fields.items():
        _compare_section(
            lambda getter=getter: getter(left),
            lambda getter=getter: getter(right),
            _join(path, name),
            context,
        )


def _computer_reference(node: orm.Node) -> Any:
    computer = getattr(node, 'computer', None)
    if computer is None:
        return None
    return {
        'uuid': str(getattr(computer, 'uuid', None)),
        'label': getattr(computer, 'label', None),
        'hostname': getattr(computer, 'hostname', None),
    }


def _compare_section(
    left_getter: Callable[[], Any],
    right_getter: Callable[[], Any],
    path: str,
    context: _ComparisonContext,
) -> None:
    left, left_error = _capture(left_getter)
    right, right_error = _capture(right_getter)
    if left_error or right_error:
        context.add(path, 'unavailable', left_error or left, right_error or right)
        return
    _compare_values(left, right, path, context)


def _capture(getter: Callable[[], Any]) -> tuple[Any, str | None]:
    try:
        return getter(), None
    except Exception as exception:
        return None, f'{exception.__class__.__name__}: {exception}'


def _compare_values(left: Any, right: Any, path: str, context: _ComparisonContext) -> None:
    if context.full or context.ignored(path):
        return
    if left is _MISSING:
        context.add(path, 'missing_left', right=right)
        return
    if right is _MISSING:
        context.add(path, 'missing_right', left=left)
        return

    if isinstance(left, Mapping) and isinstance(right, Mapping):
        for key in sorted(set(left) | set(right), key=str):
            _compare_values(
                left.get(key, _MISSING),
                right.get(key, _MISSING),
                _mapping_path(path, key),
                context,
            )
        return
    if _is_sequence(left) and _is_sequence(right):
        if type(left) is not type(right):
            context.add(path, 'type_mismatch', type(left).__name__, type(right).__name__)
        for index in range(max(len(left), len(right))):
            left_item = left[index] if index < len(left) else _MISSING
            right_item = right[index] if index < len(right) else _MISSING
            _compare_values(left_item, right_item, f'{path}[{index}]', context)
        return
    if isinstance(left, bool) or isinstance(right, bool):
        if type(left) is not type(right) or left != right:
            context.add(path, 'changed', left, right)
        return
    if isinstance(left, Real) and isinstance(right, Real):
        if not _numbers_equal(left, right, context.options.rtol, context.options.atol):
            context.add(path, 'changed', left, right)
        return
    if type(left) is not type(right):
        context.add(path, 'type_mismatch', left, right, f'{type(left).__name__} vs {type(right).__name__}')
        return

    try:
        equal = left == right
        equal = bool(equal.all()) if hasattr(equal, 'all') else bool(equal)
    except Exception:
        equal = False
    if not equal:
        context.add(path, 'changed', left, right)


def _numbers_equal(left: Real, right: Real, rtol: float, atol: float) -> bool:
    try:
        if math.isnan(float(left)) and math.isnan(float(right)):
            return True
    except (TypeError, ValueError, OverflowError):
        pass
    return math.isclose(left, right, rel_tol=rtol, abs_tol=atol)


def _is_sequence(value: Any) -> bool:
    return isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))


def _compare_repository(
    left: orm.Node,
    right: orm.Node,
    path: str,
    context: _ComparisonContext,
) -> None:
    left_hash, left_error = _capture(left.base.repository.hash)
    right_hash, right_error = _capture(right.base.repository.hash)
    if left_error or right_error:
        context.add(_join(path, 'hash'), 'unavailable', left_error or left_hash, right_error or right_hash)
        return
    if left_hash == right_hash:
        return

    left_manifest, left_error = _capture(lambda: _repository_manifest(left))
    right_manifest, right_error = _capture(lambda: _repository_manifest(right))
    if left_error or right_error:
        context.add(_join(path, 'files'), 'unavailable', left_error or left_manifest, right_error or right_manifest)
        context.add(_join(path, 'hash'), 'changed', left_hash, right_hash)
        return
    before = len(context.entries)
    _compare_values(left_manifest, right_manifest, _join(path, 'files'), context)
    if len(context.entries) == before:
        context.add(_join(path, 'hash'), 'changed', left_hash, right_hash)


def _repository_manifest(node: orm.Node) -> dict[str, Any]:
    manifest = {}
    repository = node.base.repository
    for path in sorted(repository.glob(), key=str):
        obj = repository.get_object(path)
        manifest[str(path)] = obj.key if obj.is_file() else '<directory>'
    return manifest


def _compare_array_data(
    left: orm.ArrayData,
    right: orm.ArrayData,
    path: str,
    context: _ComparisonContext,
) -> None:
    import numpy

    left_names, left_error = _capture(lambda: sorted(left.get_arraynames()))
    right_names, right_error = _capture(lambda: sorted(right.get_arraynames()))
    if left_error or right_error:
        context.add(path, 'unavailable', left_error or left_names, right_error or right_names)
        return

    for name in sorted(set(left_names) | set(right_names)):
        array_path = _join(path, name)
        if name not in left_names:
            context.add(array_path, 'missing_left', right={'array': name})
            continue
        if name not in right_names:
            context.add(array_path, 'missing_right', left={'array': name})
            continue
        left_array, left_error = _capture(lambda name=name: left.get_array(name))
        right_array, right_error = _capture(lambda name=name: right.get_array(name))
        if left_error or right_error:
            context.add(array_path, 'unavailable', left_error or left_array, right_error or right_array)
            continue
        if left_array.shape != right_array.shape:
            context.add(_join(array_path, 'shape'), 'changed', left_array.shape, right_array.shape)
            continue
        if left_array.dtype != right_array.dtype:
            context.add(_join(array_path, 'dtype'), 'changed', str(left_array.dtype), str(right_array.dtype))

        numeric = (
            numpy.issubdtype(left_array.dtype, numpy.number)
            and numpy.issubdtype(right_array.dtype, numpy.number)
        )
        if numeric:
            equal = numpy.isclose(
                left_array, right_array,
                rtol=context.options.rtol,
                atol=context.options.atol,
                equal_nan=True,
            )
        else:
            equal = numpy.equal(left_array, right_array)
        if bool(numpy.all(equal)):
            continue

        indices = numpy.argwhere(numpy.logical_not(equal))
        sample_indices = [tuple(int(value) for value in index) for index in indices[:5]]
        left_samples = [
            {'index': index, 'value': _serialise_value(left_array[index])}
            for index in sample_indices
        ]
        right_samples = [
            {'index': index, 'value': _serialise_value(right_array[index])}
            for index in sample_indices
        ]
        detail = f'{len(indices)} differing element(s)'
        if numeric:
            try:
                maximum = float(numpy.max(numpy.abs(left_array - right_array)))
                detail += f', max_abs_difference={maximum:g}'
            except (TypeError, ValueError):
                pass
        context.add(
            _join(array_path, 'values'),
            'array_changed',
            {'shape': left_array.shape, 'samples': left_samples},
            {'shape': right_array.shape, 'samples': right_samples},
            detail,
        )


def _compare_links(
    left: orm.ProcessNode,
    right: orm.ProcessNode,
    *,
    direction: str,
    link_types: tuple[LinkType, ...],
    path: str,
    child_depth: int,
    context: _ComparisonContext,
) -> None:
    left_links, left_error = _capture(lambda: _link_map(left, direction, link_types))
    right_links, right_error = _capture(lambda: _link_map(right, direction, link_types))
    if left_error or right_error:
        context.add(path, 'unavailable', left_error or left_links, right_error or right_links)
        return

    for label in sorted(set(left_links) | set(right_links)):
        left_nodes = left_links.get(label, [])
        right_nodes = right_links.get(label, [])
        base_path = _join(path, label.replace('__', '.'))
        count = max(len(left_nodes), len(right_nodes))
        for index in range(count):
            item_path = base_path if count == 1 else f'{base_path}[{index}]'
            if index >= len(left_nodes):
                context.add(item_path, 'missing_left', right=_node_reference(right_nodes[index]))
            elif index >= len(right_nodes):
                context.add(item_path, 'missing_right', left=_node_reference(left_nodes[index]))
            else:
                _compare_node(left_nodes[index], right_nodes[index], item_path, child_depth, context)


def _link_map(
    node: orm.ProcessNode,
    direction: str,
    link_types: tuple[LinkType, ...],
) -> dict[str, list[orm.Node]]:
    if direction == 'incoming':
        manager = node.base.links.get_incoming(link_type=link_types)
    elif direction == 'outgoing':
        manager = node.base.links.get_outgoing(link_type=link_types)
    else:
        raise ValueError(f'Unknown link direction: {direction}')

    result: dict[str, list[orm.Node]] = defaultdict(list)
    for triple in manager.all():
        result[triple.link_label].append(triple.node)
    for linked_nodes in result.values():
        linked_nodes.sort(
            key=lambda linked: (
                getattr(linked, 'pk', -1) or -1,
                str(getattr(linked, 'uuid', '')),
            )
        )
    return dict(result)


def _join(path: str, component: str) -> str:
    return f'{path}.{component}' if path else component


def _mapping_path(path: str, key: Any) -> str:
    """Append a mapping key without making dotted paths ambiguous."""
    text = str(key)
    if text.isidentifier():
        return _join(path, text)
    return f'{path}[{text!r}]'


def _serialise_value(value: Any) -> Any:
    if is_dataclass(value):
        return {key: _serialise_value(item) for key, item in asdict(value).items()}
    if isinstance(value, Mapping):
        return {str(key): _serialise_value(item) for key, item in value.items()}
    if _is_sequence(value):
        return [_serialise_value(item) for item in value]
    if isinstance(value, (datetime, date, PurePath)):
        return str(value)
    if hasattr(value, 'item'):
        try:
            return value.item()
        except (TypeError, ValueError):
            pass
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return repr(value)


def _preview(value: Any, maximum: int = 240) -> str:
    rendered = repr(_serialise_value(value))
    if len(rendered) <= maximum:
        return rendered
    return rendered[: maximum - 3] + '...'


def _entry_category(entry: DiffEntry) -> str:
    """Classify a leaf difference by scientific relevance and provenance role."""
    path = entry.path
    if any(part in path for part in _RUNTIME_PATH_PARTS):
        return 'runtime'
    if '.repository.files.source_file' in path or ".repository.files['source_file']" in path:
        return 'process'
    if path.startswith('$.inputs.') or '.inputs.' in path:
        return 'inputs'
    if (path.startswith('$.outputs.') or '.outputs.' in path) and '.repository.' in path:
        return 'output_files'
    if path.startswith('$.outputs.') or '.outputs.' in path:
        return 'outputs'
    if '.repository.' in path:
        return 'files'
    if '.attributes.' in path or path.endswith('.attributes'):
        return 'attributes'
    if '.extras.' in path or path.endswith('.extras'):
        return 'extras'
    if '.metadata.' in path or path.endswith('.metadata'):
        return 'metadata'
    return 'process'


def _format_grouped_entries(entries: list[DiffEntry]) -> list[str]:
    """Format entries under process-scope subheadings."""
    by_scope: dict[str, list[tuple[str, DiffEntry]]] = defaultdict(list)
    for entry in entries:
        scope, relative_path = _split_process_scope(entry.path)
        by_scope[scope].append((relative_path, entry))

    lines = []
    for scope, scoped_entries in by_scope.items():
        lines.append(f'  {scope}')
        for relative_path, entry in scoped_entries:
            lines.append(f'    {_format_entry(entry, path=relative_path)}')
    return lines


def _split_process_scope(path: str) -> tuple[str, str]:
    """Split a full diff path into process scope and node-local path."""
    text = path.removeprefix('$.')
    boundaries = (
        '.inputs.',
        '.outputs.',
        '.attributes.',
        '.extras.',
        '.metadata.',
        '.repository.',
        '.arrays.',
    )
    positions = [(text.find(boundary), boundary) for boundary in boundaries if boundary in text]
    if positions:
        position, boundary = min(positions, key=lambda item: item[0])
        process_part = text[:position]
        relative = text[position + 1:]
    else:
        process_part = ''
        relative = text

    if process_part.startswith('called.'):
        process_part = process_part[len('called.'):]
    scope = process_part.replace('.called.', ' / ') or 'ROOT'
    return scope, relative


def _format_entry(entry: DiffEntry, path: str | None = None) -> str:
    """Format one difference with a compact marker."""
    markers = {
        'missing_left': '+',
        'missing_right': '-',
        'type_mismatch': '!',
        'unavailable': '?',
    }
    marker = markers.get(entry.kind, '~')
    display_path = entry.path if path is None else path
    if entry.kind == 'missing_left':
        message = f'{marker} {display_path}: {_preview(entry.right)}'
    elif entry.kind == 'missing_right':
        message = f'{marker} {display_path}: {_preview(entry.left)}'
    else:
        message = f'{marker} {display_path}: {_preview(entry.left)} != {_preview(entry.right)}'
    if entry.detail:
        message += f' ({entry.detail})'
    return message


__all__ = [
    'CompareOptions',
    'DiffEntry',
    'NodeDiff',
    'NodeReference',
    'compare',
    'compare_nodes',
]
