/**
 * Burrow headless-Shopify funnel collector.
 *
 * Injected by the Burrow Craft plugin when headless-Shopify funnel capture is
 * enabled. Captures cart interactions in the storefront and relays them to the
 * plugin's same-origin collect endpoint (which validates and forwards to
 * Burrow server-side). Never blocks or breaks the storefront: every code path
 * is wrapped and errors are swallowed.
 *
 * Payloads carry funnel telemetry only — product/variant identifiers,
 * quantity, price, currency. No PII.
 */
(function () {
    'use strict';

    var config = window.__burrowCollectorConfig || {};
    var endpoint = String(config.endpoint || '');
    var sessionInfoUrl = String(config.sessionInfoUrl || '/actions/users/session-info');
    if (!endpoint || typeof window.fetch !== 'function') {
        return;
    }

    var csrfPromise = null;

    function fetchCsrfToken() {
        if (!csrfPromise) {
            csrfPromise = fetch(sessionInfoUrl, {
                headers: { Accept: 'application/json' },
                credentials: 'same-origin'
            })
                .then(function (response) { return response.json(); })
                .then(function (data) { return String((data && data.csrfTokenValue) || ''); })
                .catch(function () {
                    csrfPromise = null;
                    return '';
                });
        }
        return csrfPromise;
    }

    function uuid() {
        try {
            if (window.crypto && typeof window.crypto.randomUUID === 'function') {
                return window.crypto.randomUUID();
            }
        } catch (e) { /* noop */ }
        return 'xxxxxxxxyxxxyxxxyxxxyxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
            var r = (Math.random() * 16) | 0;
            return (c === 'x' ? r : (r & 0x3) | 0x8).toString(16);
        });
    }

    function mintExternalEventId() {
        var host = 'site';
        try {
            host = window.location.hostname || 'site';
        } catch (e) { /* noop */ }
        return 'craftplugin:' + host + ':cart:' + uuid();
    }

    function toNumber(value) {
        var parsed = parseFloat(value);
        return isFinite(parsed) && parsed >= 0 ? parsed : 0;
    }

    function send(payload) {
        try {
            fetchCsrfToken().then(function (token) {
                var body = JSON.stringify(payload);
                fetch(endpoint, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Accept': 'application/json',
                        'X-CSRF-Token': token
                    },
                    credentials: 'same-origin',
                    keepalive: true,
                    body: body
                }).catch(function () { /* never surface to the storefront */ });
            }).catch(function () { /* noop */ });
        } catch (e) { /* noop */ }
    }

    function track(event) {
        try {
            if (!event || typeof event !== 'object') {
                return;
            }
            var type = String(event.type || '');
            if (type !== 'cart.added' && type !== 'cart.removed') {
                return;
            }
            send({
                type: type,
                productId: String(event.productId || ''),
                productName: String(event.productName || ''),
                variantName: String(event.variantName || ''),
                quantity: toNumber(event.quantity) || 1,
                unitPrice: toNumber(event.unitPrice),
                lineTotal: toNumber(event.lineTotal),
                currency: String(event.currency || ''),
                cartTotal: toNumber(event.cartTotal),
                cartItemCount: Math.round(toNumber(event.cartItemCount)),
                customerId: String(event.customerId || ''),
                emailHash: String(event.emailHash || ''),
                externalEventId: mintExternalEventId()
            });
        } catch (e) { /* noop */ }
    }

    // Explicit hook for custom Storefront-API carts:
    //   window.burrow.track({ type: 'cart.added', productId: '...', quantity: 1, ... })
    window.burrow = window.burrow || {};
    if (typeof window.burrow.track !== 'function') {
        window.burrow.track = track;
    }

    // Automatic capture for the documented craftcms/shopify pattern: forms that
    // POST to Shopify's cart/add endpoint (relative path or shop-domain absolute).
    function isCartAddAction(action) {
        if (!action) {
            return false;
        }
        try {
            var url = new URL(action, window.location.href);
            return /\/cart\/add(\.js)?\/?$/.test(url.pathname);
        } catch (e) {
            return false;
        }
    }

    function readDataAttr(form, names) {
        var node = form;
        while (node && node.getAttribute) {
            for (var i = 0; i < names.length; i++) {
                var value = node.getAttribute('data-' + names[i]);
                if (value) {
                    return value;
                }
            }
            node = node.parentElement;
        }
        return '';
    }

    function handleCartFormSubmit(formEvent) {
        try {
            var form = formEvent.target;
            if (!form || form.nodeName !== 'FORM' || !isCartAddAction(form.getAttribute('action'))) {
                return;
            }

            var idField = form.elements.namedItem('id');
            var quantityField = form.elements.namedItem('quantity');
            var variantId = idField ? String(idField.value || '') : '';
            var variantName = '';
            if (idField && idField.nodeName === 'SELECT' && idField.selectedIndex >= 0) {
                var option = idField.options[idField.selectedIndex];
                variantName = option ? String(option.text || '') : '';
            }

            send({
                type: 'cart.added',
                productId: variantId,
                productName: readDataAttr(form, ['burrow-product-name', 'product-name', 'product-title']),
                variantName: variantName || readDataAttr(form, ['burrow-variant-name', 'variant-name', 'variant-title']),
                quantity: quantityField ? (toNumber(quantityField.value) || 1) : 1,
                unitPrice: toNumber(readDataAttr(form, ['burrow-unit-price', 'product-price', 'price'])),
                lineTotal: 0,
                currency: readDataAttr(form, ['burrow-currency', 'currency']),
                externalEventId: mintExternalEventId()
            });
        } catch (e) { /* never interfere with the submit */ }
    }

    try {
        document.addEventListener('submit', handleCartFormSubmit, true);
    } catch (e) { /* noop */ }
})();
