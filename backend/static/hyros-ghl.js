/**
 * Mini Hyros — GoHighLevel Integration
 *
 * This extends the main hyros.js pixel with GHL-specific features:
 *  - Auto-detects GHL form submissions and fires hyros.conversion("Lead")
 *  - Auto-detects GHL calendar bookings and fires hyros.conversion("Booking")
 *  - Auto-captures email from GHL form fields for identity stitching
 *  - Works on GHL funnels, websites, and embedded forms
 *
 * USAGE: Add AFTER the main pixel on all GHL funnel pages:
 *   <script src="https://YOUR_DOMAIN/t/hyros.js" data-token="YOUR_TOKEN" data-endpoint="https://YOUR_DOMAIN"></script>
 *   <script src="https://YOUR_DOMAIN/t/hyros-ghl.js"></script>
 */
(function () {
  "use strict";

  // Wait for main hyros pixel to load
  function waitForHyros(cb, attempts) {
    if (window.hyros && window.hyros.identify) {
      cb();
    } else if (attempts > 0) {
      setTimeout(function () { waitForHyros(cb, attempts - 1); }, 100);
    }
  }

  waitForHyros(function () {

    // ── Auto-capture email from GHL forms ────────────────────────────────
    function findEmailInForm(form) {
      var inputs = form.querySelectorAll('input[type="email"], input[name*="email"], input[name*="Email"]');
      for (var i = 0; i < inputs.length; i++) {
        if (inputs[i].value && inputs[i].value.indexOf("@") > -1) {
          return inputs[i].value.trim();
        }
      }
      // Fallback: check all inputs for email pattern
      var allInputs = form.querySelectorAll("input[type='text'], input[type='email']");
      for (var j = 0; j < allInputs.length; j++) {
        var val = allInputs[j].value || "";
        if (/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(val)) {
          return val.trim();
        }
      }
      return "";
    }

    function findPhoneInForm(form) {
      var inputs = form.querySelectorAll('input[type="tel"], input[name*="phone"], input[name*="Phone"]');
      for (var i = 0; i < inputs.length; i++) {
        if (inputs[i].value) return inputs[i].value.trim();
      }
      return "";
    }

    function findNameInForm(form) {
      var nameInputs = form.querySelectorAll('input[name*="name"], input[name*="Name"]');
      var parts = [];
      for (var i = 0; i < nameInputs.length; i++) {
        if (nameInputs[i].value) parts.push(nameInputs[i].value.trim());
      }
      return parts.join(" ");
    }

    // ── Hook into GHL form submissions ───────────────────────────────────
    // GHL uses specific form classes and IDs
    function hookForms() {
      var forms = document.querySelectorAll(
        'form, [data-form-id], .hl-form, #inline-form, .form-builder'
      );

      forms.forEach(function (form) {
        // Mirror hyros.js: skip checkout/payment/calendar forms so they never
        // get a Lead conversion (those are handled by the order-form / calendar
        // hooks and by the server-side payment webhook).
        if (form.matches && form.matches('.order-form, .payment-form, [data-product-id], #order-form, .hl-order-form, .checkout-form, form[action*="checkout"], form[action*="payment"], form[id*="checkout"], form[class*="checkout"], [data-calendar-id], .hl-calendar, .calendar-widget')) return;
        if (form._hyrosGhlHooked) return;
        form._hyrosGhlHooked = true;

        form.addEventListener("submit", function (e) {
          var email = findEmailInForm(form);
          var name = findNameInForm(form);
          var phone = findPhoneInForm(form);

          // No contact info captured — don't fire a Lead for an empty submit.
          if (!email && !phone) return;

          if (email) {
            hyros.identify(email);
          }

          // Determine form name from GHL attributes
          var formName =
            form.getAttribute("data-form-name") ||
            form.getAttribute("data-form-id") ||
            form.getAttribute("name") ||
            form.getAttribute("id") ||
            document.title ||
            "GHL Form";

          hyros.event("FormSubmit", {
            form_name: formName,
            email: email,
            name: name,
            phone: phone,
            page: window.location.pathname,
          });

          // Also track as a Lead conversion
          hyros.conversion({
            type: "Lead",
            value: 0,
            order_id: "lead-" + Date.now(),
            email: email,
          });
        });
      });
    }

    // ── Hook into GHL calendar bookings ──────────────────────────────────
    function hookCalendar() {
      // GHL calendar widget uses specific selectors
      var calendarBtns = document.querySelectorAll(
        '.calendar-widget button[type="submit"], ' +
        '.hl-calendar button[type="submit"], ' +
        '[data-calendar-id] button[type="submit"], ' +
        '.booking-confirm-btn, ' +
        '#confirm-booking'
      );

      calendarBtns.forEach(function (btn) {
        if (btn._hyrosGhlHooked) return;
        btn._hyrosGhlHooked = true;

        btn.addEventListener("click", function () {
          // Find the parent form/container
          var container = btn.closest("form") || btn.closest("[data-calendar-id]") || btn.parentElement;
          if (!container) return;

          var email = findEmailInForm(container);
          if (email) {
            hyros.identify(email);
          }

          var calendarName =
            container.getAttribute("data-calendar-name") ||
            container.getAttribute("data-calendar-id") ||
            "GHL Calendar";

          hyros.event("BookingConfirmed", {
            calendar: calendarName,
            email: email,
            page: window.location.pathname,
          });

          hyros.conversion({
            type: "Booking",
            value: 0,
            order_id: "booking-" + Date.now(),
            email: email,
          });
        });
      });
    }

    // ── Detect GHL order forms / payment pages ───────────────────────────
    function hookOrderForms() {
      var paymentForms = document.querySelectorAll(
        '.order-form, .payment-form, [data-product-id], ' +
        '#order-form, .hl-order-form, .checkout-form'
      );

      paymentForms.forEach(function (form) {
        if (form._hyrosGhlPayHooked) return;
        form._hyrosGhlPayHooked = true;

        form.addEventListener("submit", function () {
          var email = findEmailInForm(form);
          if (email) {
            hyros.identify(email);
          }

          // Try to get price from the form
          var priceEl = form.querySelector(
            '[data-price], .price, .total, .order-total, [class*="price"]'
          );
          var price = 0;
          if (priceEl) {
            var priceText = priceEl.textContent || priceEl.getAttribute("data-price") || "0";
            price = parseFloat(priceText.replace(/[^0-9.]/g, "")) || 0;
          }

          // Also check hidden inputs
          var hiddenPrice = form.querySelector(
            'input[name="amount"], input[name="price"], input[name="total"]'
          );
          if (hiddenPrice && hiddenPrice.value) {
            price = parseFloat(hiddenPrice.value) || price;
          }

          // Do NOT fire a Purchase on SUBMIT — the payment hasn't succeeded yet,
          // so declined cards and retries would each create a phantom revenue row
          // with a synthetic "ghl-order-"+Date.now() id. Fire a non-revenue
          // CheckoutSubmit signal and let the GHL PaymentReceived / Stripe webhook
          // record the actual order as the source of truth for revenue.
          hyros.event("CheckoutSubmit", {
            value: price,
            email: email,
            page: window.location.pathname,
          });
        });
      });
    }

    // ── Auto-detect GHL thank-you page ───────────────────────────────────
    function detectThankYouPage() {
      var url = window.location.href.toLowerCase();
      var path = window.location.pathname.toLowerCase();
      var title = (document.title || "").toLowerCase();

      var isThankYou = (
        path.indexOf("thank") > -1 ||
        path.indexOf("thankyou") > -1 ||
        path.indexOf("thank-you") > -1 ||
        path.indexOf("confirmation") > -1 ||
        path.indexOf("order-confirmed") > -1 ||
        url.indexOf("thank") > -1 ||
        title.indexOf("thank you") > -1 ||
        title.indexOf("order confirmed") > -1
      );

      if (isThankYou) {
        // Try to find order details on the page
        var email = "";
        var emailEl = document.querySelector('[data-email], .customer-email');
        if (emailEl) email = emailEl.textContent || emailEl.getAttribute("data-email") || "";

        // Check URL params for order info (GHL sometimes passes these)
        var urlParams = new URLSearchParams(window.location.search);
        email = email || urlParams.get("email") || urlParams.get("contact_email") || "";

        if (email) {
          hyros.identify(email);
        }

        hyros.event("ThankYouPage", {
          page: window.location.pathname,
          email: email,
        });
      }
    }

    // ── Initialize ───────────────────────────────────────────────────────
    function init() {
      hookForms();
      hookCalendar();
      hookOrderForms();
      detectThankYouPage();
    }

    // Run immediately
    init();

    // Re-run when DOM changes (GHL loads content dynamically)
    var observer = new MutationObserver(function (mutations) {
      var shouldRehook = false;
      for (var i = 0; i < mutations.length; i++) {
        if (mutations[i].addedNodes.length > 0) {
          shouldRehook = true;
          break;
        }
      }
      if (shouldRehook) {
        setTimeout(init, 200);
      }
    });

    // document.body is null when this script loads in <head>; observing null
    // throws and kills the MutationObserver. Observe now if the body exists,
    // otherwise wait for DOMContentLoaded.
    if (document.body) {
      observer.observe(document.body, { childList: true, subtree: true });
    } else {
      document.addEventListener("DOMContentLoaded", function () {
        if (document.body) {
          observer.observe(document.body, { childList: true, subtree: true });
        }
      });
    }

    // Also re-hook on GHL page navigation (GHL uses client-side routing)
    window.addEventListener("hashchange", function () { setTimeout(init, 200); });

  }, 50); // max 5 seconds waiting for main pixel
})();
