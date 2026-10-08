"""Semantic observations in a private Chromium context using synthetic DOM only."""

import json

import pytest
import pytest_asyncio

from services.browser_recordings import RecordingSpool
from services.browser_service import BrowserService


@pytest_asyncio.fixture
async def semantic_browser(tmp_path):
    service = BrowserService()
    service.browser_type = "chromium"
    service.browser_channel = None
    service.headless = True
    service.recordings = RecordingSpool(tmp_path / "recordings")
    await service.startup()
    try:
        session = await service.get_or_create_session("semantic-fixture")
        await session.context.route("**/*", lambda route: route.abort())
        yield service, session.page
    finally:
        await service.shutdown()


async def capture(service, selector=None):
    return await service.snapshot_semantic(
        "semantic-fixture", selector=selector, strict_session=True
    )


def named(snapshot, name):
    return next(item for item in snapshot["interactive"] if item["name"] == name)


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_semantic_state_changes_are_observable_in_both_components(semantic_browser):
    service, page = semantic_browser
    await page.set_content(
        '<label><input id="agree" type="checkbox">Agree</label>'
        '<button id="details" aria-expanded="false">Details</button>'
    )
    before = await capture(service)
    native_before = await page.locator("body").aria_snapshot()
    await page.locator("#agree").check()
    await page.locator("#details").evaluate(
        '(el) => el.setAttribute("aria-expanded", "true")'
    )
    after = await capture(service)
    native_after = await page.locator("body").aria_snapshot()

    assert native_before != native_after
    assert before["tree"] != after["tree"]
    assert before["interactive"] != after["interactive"]
    assert named(before, "Agree")["checked"] is False
    assert named(after, "Agree")["checked"] is True
    assert named(before, "Details")["expanded"] is False
    assert named(after, "Details")["expanded"] is True
    assert "checkbox: Agree" in after["tree"]["safe_dom"]
    assert "[checked=true]" in after["tree"]["safe_dom"]
    assert "[expanded=true]" in after["tree"]["safe_dom"]
    assert before["capture_status"] == after["capture_status"] == "complete"


@pytest.mark.asyncio
@pytest.mark.timeout(30)
@pytest.mark.parametrize(
    "html,initial,updated",
    [
        ('<input id="root" aria-label="Name" value="Alice">', "Alice", "Bob"),
        ('<textarea id="root" aria-label="Name">Alice</textarea>', "Alice", "Bob"),
        ('<select id="root" aria-label="Name"><option value="a">Alpha</option>'
         '<option value="b">Beta</option></select>', "a", "b"),
    ],
)
async def test_semantic_scope_root_preserves_live_normal_value(
    semantic_browser, html, initial, updated
):
    service, page = semantic_browser
    await page.set_content(html)
    before = await capture(service, "#root")
    await page.locator("#root").evaluate("(el, value) => el.value = value", updated)
    after = await capture(service, "#root")

    assert named(before, "Name")["value"] == initial
    assert named(after, "Name")["value"] == updated
    assert sum(item["name"] == "Name" for item in after["interactive"]) == 1
    assert f'value="{initial}"' in before["tree"]["safe_dom"]
    assert f'value="{updated}"' in after["tree"]["safe_dom"]
    if html.startswith("<textarea"):
        assert initial not in after["tree"]["safe_dom"]
    assert after["capture_status"] == "complete"


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_semantic_native_mixed_radio_and_multiple_selected_options(semantic_browser):
    service, page = semantic_browser
    await page.set_content(
        '<input id="mixed" type="checkbox" aria-label="Mixed" aria-checked="false">'
        '<input id="radio" type="radio" aria-label="Radio">'
        '<select id="items" multiple aria-label="Items">'
        '<option value="a" selected>Alpha</option><option value="b">Beta</option>'
        '</select>'
    )
    before = await capture(service)
    await page.evaluate("""() => {
        document.querySelector('#mixed').indeterminate = true;
        document.querySelector('#radio').checked = true;
        document.querySelectorAll('option')[1].selected = true;
    }""")
    after = await capture(service)

    assert named(before, "Mixed")["checked"] is False
    assert named(after, "Mixed")["checked"] == "mixed"
    assert named(before, "Radio")["checked"] is False
    assert named(after, "Radio")["checked"] is True
    assert named(before, "Beta")["selected"] is False
    assert named(after, "Alpha")["selected"] is True
    assert named(after, "Beta")["selected"] is True
    assert named(before, "Items")["value"] == named(after, "Items")["value"] == "a"
    assert "[checked=mixed]" in after["tree"]["safe_dom"]
    assert "option: Beta [selected=true]" in after["tree"]["safe_dom"]
    assert before["tree"] != after["tree"]


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_semantic_aria_states_include_tree_and_menu_roles(semantic_browser):
    service, page = semantic_browser
    cases = [
        ("checkbox", "checked", "mixed"),
        ("menuitemcheckbox", "checked", "mixed"),
        ("menuitemradio", "checked", "true"),
        ("switch", "checked", "true"),
        ("button", "pressed", "mixed"),
        ("treeitem", "expanded", "true"),
        ("tab", "selected", "true"),
        ("option", "selected", "true"),
        # The safe tree must also retain states outside the interactive selector set.
        ("row", "selected", "true"),
    ]
    await page.set_content("".join(
        f'<div id="state-{i}" role="{role}" aria-label="State {i}" '
        f'aria-{state}="false"></div>'
        for i, (role, state, _) in enumerate(cases)
    ))
    before = await capture(service)
    for i, (_, state, value) in enumerate(cases):
        await page.locator(f"#state-{i}").evaluate(
            "(el, pair) => el.setAttribute(...pair)", [f"aria-{state}", value]
        )
    after = await capture(service)

    for i, (role, state, value) in enumerate(cases):
        assert f'{role}: State {i} [{state}=false]' in before["tree"]["safe_dom"]
        assert f'{role}: State {i} [{state}={value}]' in after["tree"]["safe_dom"]
        if role != "row":
            assert named(before, f"State {i}")[state] is False
            assert named(after, f"State {i}")[state] == (True if value == "true" else value)


@pytest.mark.asyncio
@pytest.mark.timeout(30)
@pytest.mark.parametrize("selector", [None, "#password"])
async def test_semantic_password_is_never_read_and_shared_values_are_read_once(
    semantic_browser, selector
):
    service, page = semantic_browser
    await page.set_content(
        '<input id="password" type="text" aria-label="Password" '
        'value="synthetic-password-canary-98417">'
        '<input id="normal" aria-label="Normal" value="visible">'
        '<input id="hidden" type="hidden" value="synthetic-hidden-canary-47189">'
    )
    await page.evaluate("""() => {
        const password = document.querySelector('#password');
        password.type = 'password';
        window.passwordReads = window.normalReads = window.hiddenReads = 0;
        Object.defineProperty(password, 'value', {get() {
            window.passwordReads++;
            throw new Error('password getter must never run');
        }});
        const getter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').get;
        Object.defineProperty(document.querySelector('#normal'), 'value', {get() {
            window.normalReads++;
            return getter.call(this);
        }});
        Object.defineProperty(document.querySelector('#hidden'), 'value', {get() {
            window.hiddenReads++;
            throw new Error('hidden values were not part of the previous projection');
        }});
    }""")
    snapshot = await capture(service, selector)

    assert snapshot["capture_status"] == "complete"
    assert "synthetic-password-canary-98417" not in json.dumps(snapshot)
    assert "synthetic-hidden-canary-47189" not in json.dumps(snapshot)
    assert await page.evaluate("window.passwordReads") == 0
    assert await page.evaluate("window.hiddenReads") == 0
    assert named(snapshot, "Password")["value_redacted"] is True
    assert "value" not in named(snapshot, "Password")
    assert "[value_redacted=true]" in snapshot["tree"]["safe_dom"]
    if selector is None:
        assert named(snapshot, "Normal")["value"] == "visible"
        assert 'value="visible"' in snapshot["tree"]["safe_dom"]
        assert await page.evaluate("window.normalReads") == 1


@pytest.mark.asyncio
@pytest.mark.timeout(30)
@pytest.mark.parametrize("count", [200, 201])
async def test_semantic_interactive_limit_includes_scope_root(semantic_browser, count):
    service, page = semantic_browser
    await page.set_content(
        '<div id="root" role="button" aria-label="Root">'
        + ''.join(f'<button aria-label="Child {i}"></button>' for i in range(count - 1))
        + '</div>'
    )
    snapshot = await capture(service, "#root")

    assert len(snapshot["interactive"]) == 200
    assert snapshot["interactive"][0]["name"] == "Root"
    assert bool(snapshot["components"]["interactive"].get("truncated")) == (count > 200)
    assert snapshot["capture_status"] == ("partial" if count > 200 else "complete")


@pytest.mark.asyncio
@pytest.mark.timeout(30)
@pytest.mark.parametrize("count", [1999, 2000])
async def test_semantic_tree_node_limit_keeps_partial_status(semantic_browser, count):
    service, page = semantic_browser
    await page.set_content('<div></div>' * count)
    snapshot = await capture(service)

    assert len(snapshot["tree"]["safe_dom"].splitlines()) == 2000
    assert bool(snapshot["components"]["safe_dom"].get("truncated")) == (count > 1999)
    assert snapshot["capture_status"] == ("partial" if count > 1999 else "complete")


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_semantic_tree_text_limit_includes_added_values_and_states(semantic_browser):
    service, page = semantic_browser
    await page.set_content(''.join(
        f'<input aria-label="Value {i}" value="{"字" * 200}" aria-expanded="true">'
        for i in range(200)
    ))
    snapshot = await capture(service)

    assert len(snapshot["interactive"]) == 200
    assert len(snapshot["tree"]["safe_dom"].encode("utf-8")) <= 65536
    assert snapshot["components"]["safe_dom"]["truncated"] is True
    assert snapshot["capture_status"] == "partial"


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_semantic_open_shadow_controls_keep_live_values_and_states(semantic_browser):
    service, page = semantic_browser
    await page.set_content('<custom-form id="host"></custom-form>')
    await page.evaluate("""() => {
        document.querySelector('#host').attachShadow({mode: 'open'}).innerHTML =
            '<label><input id="agree" type="checkbox">Agree</label>' +
            '<input aria-label="Name" value="Alice">' +
            '<button aria-expanded="false">Save</button>';
    }""")
    before = await capture(service)
    assert 'textbox "Name": Alice' in await page.locator('body').aria_snapshot()
    await page.locator('#agree').check()
    await page.get_by_role('textbox', name='Name').fill('Bob')
    await page.get_by_role('button', name='Save').evaluate(
        '(el) => el.setAttribute("aria-expanded", "true")'
    )
    after = await capture(service)
    assert named(before, 'Agree')['checked'] is False
    assert named(after, 'Agree')['checked'] is True
    assert named(before, 'Name')['value'] == 'Alice'
    assert named(after, 'Name')['value'] == 'Bob'
    assert named(after, 'Save')['expanded'] is True
    assert 'textbox: Name value="Bob"' in after['tree']['safe_dom']
    assert 'button: Save [expanded=true]' in after['tree']['safe_dom']
    assert before['capture_status'] == after['capture_status'] == 'complete'


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_semantic_nested_shadow_names_stay_in_scope_without_slot_duplication(semantic_browser):
    service, page = semantic_browser
    await page.set_content(
        '<span id="label">Outside label</span><button>Outside button</button>'
        '<custom-form id="host"><button slot="content">Light button</button></custom-form>'
    )
    await page.evaluate("""() => {
        const outer = document.querySelector('#host').attachShadow({mode: 'open'});
        outer.innerHTML = '<span id="label">Outer label</span>' +
            '<button aria-labelledby="label"></button><slot name="content"></slot>' +
            '<custom-form id="nested"></custom-form>';
        outer.querySelector('#nested').attachShadow({mode: 'open'}).innerHTML =
            '<span id="label">Inner label</span><input aria-labelledby="label" value="nested">';
    }""")
    snapshot = await capture(service, '#host')
    assert [item['name'] for item in snapshot['interactive']] == [
        'Light button', 'Outer label', 'Inner label',
    ]
    assert named(snapshot, 'Inner label')['value'] == 'nested'
    tree = snapshot['tree']['safe_dom']
    assert 'button: Outer label' in tree
    assert 'textbox: Inner label value="nested"' in tree
    assert tree.count('button: Light button') == 1
    assert 'Outside' not in json.dumps(snapshot)
    assert snapshot['capture_status'] == 'complete'


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_semantic_shadow_password_and_hidden_values_are_never_read(semantic_browser):
    service, page = semantic_browser
    await page.set_content('<custom-form id="host"></custom-form>')
    await page.evaluate("""() => {
        const root = document.querySelector('#host').attachShadow({mode: 'open'});
        root.innerHTML = '<input id="password" type="text" aria-label="Password" value="shadow-secret-773">' +
            '<input id="normal" aria-label="Normal" value="visible">' +
            '<input id="hidden" type="hidden" value="shadow-hidden-114">';
        root.querySelector('#password').type = 'password';
        window.reads = {password: 0, hidden: 0, normal: 0};
        const getter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').get;
        for (const id of ['password', 'hidden', 'normal']) {
            Object.defineProperty(root.querySelector('#' + id), 'value', {get() {
                window.reads[id]++;
                if (id !== 'normal') throw new Error('sensitive value must never be read');
                return getter.call(this);
            }});
        }
    }""")
    snapshot = await capture(service, '#host')
    assert await page.evaluate('window.reads') == {'password': 0, 'hidden': 0, 'normal': 1}
    assert named(snapshot, 'Password')['value_redacted'] is True
    assert 'value' not in named(snapshot, 'Password')
    assert named(snapshot, 'Normal')['value'] == 'visible'
    assert 'textbox: Normal value="visible"' in snapshot['tree']['safe_dom']
    assert 'shadow-secret-773' not in json.dumps(snapshot)
    assert 'shadow-hidden-114' not in json.dumps(snapshot)
    assert snapshot['capture_status'] == 'complete'


@pytest.mark.asyncio
@pytest.mark.timeout(30)
@pytest.mark.parametrize('count', [200, 201])
async def test_semantic_shadow_interactive_budget_is_shared_with_light_dom(semantic_browser, count):
    service, page = semantic_browser
    await page.set_content(
        ''.join(f'<button aria-label="Light {i}"></button>' for i in range(100))
        + '<custom-form id="host"></custom-form>'
    )
    await page.evaluate("""count => {
        const root = document.querySelector('#host').attachShadow({mode: 'open'});
        root.innerHTML = Array.from({length: count - 100}, (_, i) =>
            '<button aria-label="Shadow ' + i + '"></button>').join('');
    }""", count)
    snapshot = await capture(service)
    assert len(snapshot['interactive']) == 200
    assert snapshot['interactive'][-1]['name'] == 'Shadow 99'
    assert bool(snapshot['components']['interactive'].get('truncated')) == (count > 200)
    assert snapshot['capture_status'] == ('partial' if count > 200 else 'complete')


@pytest.mark.asyncio
@pytest.mark.timeout(30)
@pytest.mark.parametrize('count', [1997, 1998])
async def test_semantic_shadow_node_budget_is_global_across_roots(semantic_browser, count):
    service, page = semantic_browser
    await page.set_content('<custom-form id="first"></custom-form><custom-form id="second"></custom-form>')
    await page.evaluate("""count => {
        document.querySelector('#first').attachShadow({mode: 'open'}).innerHTML = '<div></div>'.repeat(1000);
        document.querySelector('#second').attachShadow({mode: 'open'}).innerHTML = '<div></div>'.repeat(count - 1000);
    }""", count)
    snapshot = await capture(service)
    # body + two hosts + all admitted shadow elements share the same 2000 nodes.
    # Preserve the public light-DOM count: body and two hosts, without shadows.
    assert snapshot['node_count'] == 3
    assert len(snapshot['tree']['safe_dom'].splitlines()) == 2000
    assert bool(snapshot['components']['safe_dom'].get('truncated')) == (count > 1997)
    assert snapshot['capture_status'] == ('partial' if count > 1997 else 'complete')


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_semantic_shadow_text_budget_keeps_interactive_collection(semantic_browser):
    service, page = semantic_browser
    await page.set_content('<custom-form id="first"></custom-form><custom-form id="second"></custom-form>')
    await page.evaluate("""() => {
        for (const id of ['first', 'second']) {
            const root = document.querySelector('#' + id).attachShadow({mode: 'open'});
            root.innerHTML = Array.from({length: 100}, (_, i) =>
                '<input aria-label="' + id + i + '" value="' + '字'.repeat(200) + '">').join('');
        }
    }""")
    snapshot = await capture(service)
    assert len(snapshot['interactive']) == 200
    assert snapshot['interactive'][-1]['name'] == 'second99'
    assert len(snapshot['tree']['safe_dom'].encode('utf-8')) <= 65536
    assert snapshot['components']['safe_dom']['truncated'] is True
    assert snapshot['capture_status'] == 'partial'


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_semantic_shadow_extension_keeps_closed_roots_and_frames_out_of_scope(semantic_browser):
    service, page = semantic_browser
    await page.set_content('<custom-form id="closed"></custom-form><iframe></iframe>')
    await page.evaluate("""() => {
        document.querySelector('#closed').attachShadow({mode: 'closed'}).innerHTML = '<button>Closed-only</button>';
        document.querySelector('iframe').contentDocument.body.innerHTML = '<button>Frame-only</button>';
    }""")
    snapshot = await capture(service)
    assert snapshot['interactive'] == []
    assert 'Closed-only' not in json.dumps(snapshot)
    assert 'Frame-only' not in json.dumps(snapshot)
    # Complete describes the declared projection, not full accessibility coverage.
    assert snapshot['capture_status'] == 'complete'


@pytest.mark.asyncio
@pytest.mark.timeout(30)
@pytest.mark.parametrize('prefix', ['elements', 'hidden', 'text'])
async def test_semantic_light_dom_tail_control_survives_tree_budget(semantic_browser, prefix):
    service, page = semantic_browser
    await page.set_content('<div id="prefix"></div><button>Save</button>')
    await page.evaluate("""kind => {
        const prefix = document.querySelector('#prefix');
        if (kind === 'text') {
            // Separate text nodes with comments so adjacent strings do not merge.
            for (let i = 0; i < 2001; i++) {
                prefix.append(document.createTextNode('text'), document.createComment('boundary'));
            }
        } else {
            prefix.hidden = kind === 'hidden';
            prefix.innerHTML = '<div></div>'.repeat(2001);
        }
    }""", prefix)
    snapshot = await capture(service)
    assert named(snapshot, 'Save')['role'] == 'button'
    assert len(snapshot['interactive']) == 1
    assert bool(snapshot['components']['safe_dom'].get('truncated')) == (prefix != 'hidden')
    assert snapshot['capture_status'] == ('complete' if prefix == 'hidden' else 'partial')
    if prefix == 'hidden':
        assert 'button: Save' in snapshot['tree']['safe_dom']


@pytest.mark.asyncio
@pytest.mark.timeout(30)
@pytest.mark.parametrize('tag', ['script', 'style', 'noscript', 'template', 'textarea'])
async def test_semantic_excluded_tree_subtrees_do_not_consume_descendant_budget(semantic_browser, tag):
    service, page = semantic_browser
    await page.set_content('<button>Save</button>')
    await page.evaluate("""tag => {
        const prefix = document.createElement(tag);
        if (tag === 'script') prefix.type = 'application/json';
        for (let i = 0; i < 2001; i++) {
            prefix.append(document.createTextNode('ignored'), document.createComment('boundary'));
        }
        document.body.prepend(prefix);
    }""", tag)
    snapshot = await capture(service)
    assert named(snapshot, 'Save')['role'] == 'button'
    assert 'button: Save' in snapshot['tree']['safe_dom']
    assert snapshot['capture_status'] == 'complete'


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_semantic_node_count_keeps_light_dom_and_hidden_descendants(semantic_browser):
    service, page = semantic_browser
    await page.set_content(
        '<div hidden><span></span><span></span><span></span></div>'
        '<custom-form id="host"></custom-form><button>Save</button>'
    )
    await page.evaluate("""() => {
        document.querySelector('#host').attachShadow({mode: 'open'}).innerHTML =
            '<div></div>'.repeat(5);
    }""")
    snapshot = await capture(service)
    assert snapshot['node_count'] == 7  # body, hidden div and 3 spans, host, button
    assert snapshot['capture_status'] == 'complete'
    scoped = await capture(service, '#host')
    assert scoped['node_count'] == 1
    assert scoped['tree']['safe_dom'].count('div') == 5
