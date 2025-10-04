/**
 * annotations.js
 *
 * Provides functionality to identify, collect data about, and visually annotate
 * interactive elements on a web page. Designed for use in web automation,
 * testing, or user guidance tools.
 * Focuses on collecting element properties, not generating brittle selectors for replay.
 */
(function () {
  "use strict";

  // --- Helper Functions ---
  function getAccessibleName(element) {
    // Using the version that worked for element 15 previously
    if (!element) return null;
    try {
      const labelledBy = element.getAttribute("aria-labelledby");
      if (labelledBy) {
        const labelElementIds = labelledBy
          .split(" ")
          .filter((id) => id.trim() !== "");
        const labelTexts = labelElementIds
          .map((id) => {
            const el = document.getElementById(id);
            return el ? (el.textContent || "").trim() : "";
          })
          .filter((text) => text !== "");
        if (labelTexts.length > 0) {
          console.log("AccName: Found by aria-labelledby");
          return labelTexts.join(" ").trim();
        }
      }
      const ariaLabel = element.getAttribute("aria-label");
      if (ariaLabel && ariaLabel.trim() !== "") {
        console.log("AccName: Found by aria-label");
        return ariaLabel.trim();
      }
      if (element.labels && element.labels.length > 0) {
        const labelText = Array.from(element.labels)
          .map((lbl) => (lbl.textContent || "").trim())
          .filter((txt) => txt)
          .join(" ");
        if (labelText) {
          console.log("AccName: Found by element.labels");
          return labelText;
        }
      }
      const role = element.getAttribute("role");
      const tagName = element.tagName.toLowerCase();
      if (role === "button" || tagName === "button") {
        const text = (element.textContent || "").trim().replace(/\s+/g, " ");
        if (text) {
          console.log(
            `AccName: Found by textContent for role/tag=button ('${text.substring(
              0,
              50
            )}...')`
          );
          return text;
        }
      }
      if (
        tagName === "img" ||
        (tagName === "input" && element.type === "image")
      ) {
        if (element.alt && element.alt.trim() !== "") {
          console.log("AccName: Found by img alt");
          return element.alt.trim();
        }
      }
      if (
        tagName === "input" &&
        (element.type === "button" ||
          element.type === "submit" ||
          element.type === "reset")
      ) {
        if (element.value && element.value.trim() !== "") {
          console.log("AccName: Found by input value");
          return element.value.trim();
        }
      }
      if (tagName === "summary") {
        const summaryText = (element.textContent || "").trim();
        if (summaryText) {
          console.log("AccName: Found by summary text");
          return summaryText;
        }
      }
      const title = element.getAttribute("title");
      if (title && title.trim() !== "") {
        console.log("AccName: Found by title attribute");
        return title.trim();
      }
      const allowTextContentRoles = [
        "link",
        "menuitem",
        "tab",
        "checkbox",
        "radio",
        "heading",
        "option",
        "treeitem",
        null,
      ];
      const tagNameAllowsTextContent = [
        "A",
        "SUMMARY",
        "H1",
        "H2",
        "H3",
        "H4",
        "H5",
        "H6",
        "LABEL",
        "P",
        "SPAN",
        "DIV",
        "LI",
        "TD",
        "TH",
        "OPTION",
        "LEGEND",
        "CAPTION",
      ];
      if (
        (role === null || allowTextContentRoles.includes(role)) &&
        tagNameAllowsTextContent.includes(element.tagName.toUpperCase())
      ) {
        const text = (element.textContent || "").trim().replace(/\s+/g, " ");
        if (text && text.length < 150) {
          console.log(
            `AccName: Found by generic textContent fallback ('${text.substring(
              0,
              50
            )}...')`
          );
          return text;
        }
      }
    } catch (e) {
      console.warn("Error getting accessible name for:", element, e);
    }
    console.log("AccName: No suitable name found.");
    return null;
  }

  // --- Main Function ---
  // REPLACE THE ENTIRE window.getInteractiveElements function with this:
  window.getInteractiveElements = function () {
    console.log(
      "Starting to get interactive elements (property collection focus)..."
    );
    const elements = [];
    const startTime = performance.now();

    // Selectors: Add .datepicker-switch
    const coreInteractiveSelectors = [
      "a",
      "button",
      'input:not([type="hidden"])',
      "select",
      "textarea",
      "details summary",
      "label",
    ];
    const roleBasedSelectors = [
      '[role="button"]',
      '[role="link"]',
      '[role="menuitem"]',
      '[role="menuitemcheckbox"]',
      '[role="menuitemradio"]',
      '[role="tab"]',
      '[role="checkbox"]',
      '[role="radio"]',
      '[role="option"]',
      '[role="combobox"]',
      '[role="slider"]',
      '[role="spinbutton"]',
      '[role="switch"]',
      '[role="searchbox"]',
      '[role="treeitem"]',
    ];
    const commonPatternsSelectors = [
      "[onclick]",
      "[tabindex]:not([tabindex^='-'])",
      "[contenteditable='true']",
      "[class*='button']",
      "[class*='btn']",
      "[class*='link']",
      "[data-ng-click]", // Keep this for ng-click elements
      // Datepicker specific selectors
      ".datepicker-days td.day:not(.disabled):not(.old):not(.new)",
      ".datepicker-months span.month:not(.disabled)",
      ".datepicker-years span.year:not(.disabled)",
      ".datepicker-switch", // Datepicker switch selector
    ];
    const tableCellSelectors = ["td", "th"]; // Keep td/th for potential grid cells/interactive headers

    // Combine selectors
    const allSelectors = [
      ...coreInteractiveSelectors,
      ...roleBasedSelectors,
      ...commonPatternsSelectors,
      ...tableCellSelectors,
    ].filter((value, index, self) => self.indexOf(value) === index); // Ensure unique

    let foundElements = [];
    try {
      const selectorString = allSelectors.join(", ");
      foundElements = Array.from(document.querySelectorAll(selectorString));
    } catch (queryError) {
      console.error(
        "Error querying elements with combined selector:",
        queryError
      );
      // Attempt fallback query (simpler set)
      try {
        console.log("Attempting fallback query...");
        const fallbackSelectors = [
          "a",
          "button",
          'input:not([type="hidden"])',
          "select",
          "textarea",
          "label",
          "[onclick]",
          "[role]",
          "[tabindex]:not([tabindex^='-'])",
          "[class*='btn']",
          "[class*='button']",
          "[data-ng-click]",
          ".datepicker-switch",
          "td",
          "th", // Add td/th here too
        ].join(", ");
        foundElements = Array.from(
          document.querySelectorAll(fallbackSelectors)
        );
        console.log(
          `Fallback query found ${foundElements.length} potential elements.`
        );
      } catch (fallbackError) {
        console.error("Fallback query also failed:", fallbackError);
        return []; // Return empty if even fallback fails
      }
    }

    const uniqueElementsMap = new Map();
    const processingStartTime = performance.now();

    for (const element of foundElements) {
      try {
        const style = window.getComputedStyle(element);
        const tagName = element.tagName.toLowerCase();
        const role = element.getAttribute("role"); // Get role early for checks

        // --- 1. Visibility and Size Check ---
        if (
          style.display === "none" ||
          style.visibility === "hidden" ||
          parseFloat(style.opacity) < 0.1
        ) {
          continue;
        }
        const rect = element.getBoundingClientRect();
        if (
          rect.width <= 1 ||
          rect.height <= 1 ||
          (rect.right <= 0 && rect.left <= 0) ||
          (rect.bottom <= 0 && rect.top <= 0) ||
          (rect.left >= window.innerWidth && rect.right >= window.innerWidth) ||
          (rect.top >= window.innerHeight && rect.bottom >= window.innerHeight)
        ) {
          continue; // Skip elements outside viewport or zero size
        }

        // --- 2. Interactivity Heuristics Check ---
        let isLikelyInteractive = false;
        const tabIndex = element.getAttribute("tabindex");
        const isFocusable = tabIndex !== null && parseInt(tabIndex, 10) >= 0;
        const hasDirectClickHandler =
          element.hasAttribute("onclick") ||
          typeof element.onclick === "function";
        const hasPointerCursor = style.cursor === "pointer";
        const hasNgClickHandler = element.hasAttribute("data-ng-click");
        const isDatepickerSwitch =
          element.classList.contains("datepicker-switch");

        // Apply heuristics in order
        if (tagName.match(/^(button|input|select|textarea|details)$/)) {
          isLikelyInteractive = true;
        } else if (tagName === "a") {
          const href = element.getAttribute("href");
          // Treat links with actual hrefs as interactive
          if (
            href &&
            href.trim() !== "#" &&
            !href.startsWith("javascript:void(0)")
          ) {
            isLikelyInteractive = true;
          }
          // Also treat link-like elements (role=button, click handlers, etc.)
          else if (
            role === "button" ||
            hasDirectClickHandler ||
            isFocusable ||
            hasPointerCursor ||
            element.classList.contains("btn") ||
            /\\bbutton\\b/i.test(element.className) ||
            hasNgClickHandler
          ) {
            isLikelyInteractive = true;
          }
        } else if (tagName === "label") {
          // Labels are interactive if they associate with an enabled control
          const control = element.control;
          if (
            control &&
            control.tagName.match(/^(input|select|textarea|button)$/i) &&
            !control.disabled
          ) {
            isLikelyInteractive = true;
          } else {
            // Or if they contain an enabled control
            const containedInput = element.querySelector(
              'input:not([type="hidden"]), button, select, textarea'
            );
            if (containedInput && !containedInput.disabled) {
              isLikelyInteractive = true;
            }
          }
        } else if (
          role &&
          role.match(
            /^(button|link|menuitem|menuitemcheckbox|menuitemradio|tab|checkbox|radio|option|combobox|slider|spinbutton|switch|searchbox|treeitem)$/i
          )
        ) {
          isLikelyInteractive = true;
        } else if (isDatepickerSwitch) {
          isLikelyInteractive = true;
        } // Explicitly mark datepicker switch
        else if (hasNgClickHandler) {
          isLikelyInteractive = true;
        } // Other ng-clicks
        else if (isFocusable) {
          isLikelyInteractive = true;
        } // Focusable elements
        else if (hasDirectClickHandler) {
          isLikelyInteractive = true;
        } // Direct onclick
        else if (element.isContentEditable) {
          isLikelyInteractive = true;
        } // Content editable
        else if (
          hasPointerCursor ||
          element.classList.contains("btn") ||
          /\\bbutton\\b/i.test(element.className)
        ) {
          // Elements with button-like appearance/cursor might be interactive containers
          if (!tagName.match(/^(body|html|main|section|article|aside|nav)$/)) {
            // Avoid large layout tags
            // Check if it DOES NOT contain an obviously interactive element already
            if (
              !element.querySelector(
                'a, button, input, select, textarea, [onclick], [role^="button"], [role^="link"]'
              )
            ) {
              isLikelyInteractive = true;
            }
          }
        }

        // Final check for LI, TD, TH (often used in lists, grids, menus)
        if (
          (tagName === "li" || tagName === "td" || tagName === "th") &&
          !isLikelyInteractive
        ) {
          // Datepicker days/months/years specifically
          const isDatepickerControl = element.matches(
            ".day:not(.disabled), .month:not(.disabled), .year:not(.disabled)"
          );
          if (isDatepickerControl) {
            isLikelyInteractive = true;
          }
          // List items/table cells with specific roles or handlers
          else if (
            (role &&
              role.match(
                /^(option|menuitem|menuitemcheckbox|menuitemradio|treeitem|gridcell|rowheader|columnheader)$/i
              )) ||
            hasNgClickHandler ||
            hasPointerCursor
          ) {
            isLikelyInteractive = true;
          } else {
            continue; // Skip non-interactive li/td/th
          }
        }

        if (!isLikelyInteractive) {
          continue; // Skip if no heuristic matched
        }

        // --- 3. Disabled Check ---
        let isEnabled = true;
        if (
          element.disabled ||
          (element.hasAttribute("aria-disabled") &&
            element.getAttribute("aria-disabled") === "true")
        ) {
          isEnabled = false;
        } else {
          // Check fieldset disabled state only if the element is inside one
          if (element.matches("fieldset *:not(legend)")) {
            // More efficient check
            let parentFieldset = element.closest("fieldset");
            if (parentFieldset && parentFieldset.disabled) {
              // Element is disabled if it's inside a disabled fieldset, UNLESS it's the child of that fieldset's first legend
              let firstLegendChild = parentFieldset.querySelector(
                "legend:first-of-type *"
              );
              if (!firstLegendChild || !firstLegendChild.contains(element)) {
                isEnabled = false;
              }
            }
          }
        }
        // We don't necessarily skip disabled elements, just record their state

        // --- 4. Collect Data ---
        const accessibleName = getAccessibleName(element); // Assuming getAccessibleName is defined elsewhere
        const textContent = element.textContent
          ? element.textContent.trim().replace(/\\s+/g, " ")
          : "";
        let labelText = null;
        if (element.labels && element.labels.length > 0) {
          labelText = Array.from(element.labels)
            .map((lbl) => (lbl.textContent || "").trim())
            .filter((txt) => txt)
            .join(" ");
        }

        // *** BEGIN: ADDED CODE FOR PARENT CONTEXT ***
        let parentInfo = null;
        if (element.parentElement) {
          const parent = element.parentElement;
          parentInfo = {
            tagName: parent.tagName.toLowerCase(),
            id: parent.id || null,
            role: parent.getAttribute("role"),
            classList: Array.from(parent.classList)
              .filter(
                (cls) =>
                  !cls.startsWith("ng-") &&
                  !["selected", "active", "focus"].includes(cls)
              )
              .slice(0, 3),
          };
        }
        // *** END: ADDED CODE FOR PARENT CONTEXT ***

        // *** BEGIN: ADDED CODE FOR ANCESTOR CONTEXT ***
        let ancestorInfo = null;
        let current = element.parentElement;
        let depth = 0;
        const significantRoles = [
          "listbox",
          "menu",
          "menubar",
          "dialog",
          "form",
          "grid",
          "table",
          "radiogroup",
          "tablist",
          "toolbar",
          "tree",
          "combobox",
        ];
        const significantTags = [
          "form",
          "table",
          "ul",
          "ol",
          "nav",
          "aside",
          "main",
          "section",
          "article",
        ]; // Lowercase

        while (current && current !== document.body && depth < 5) {
          const role = current.getAttribute("role");
          const tagName = current.tagName.toLowerCase();

          if (role && significantRoles.includes(role)) {
            ancestorInfo = {
              tagName: tagName,
              id: current.id || null,
              role: role,
            };
            break; // Found role-based ancestor
          }
          if (!ancestorInfo && significantTags.includes(tagName)) {
            ancestorInfo = {
              tagName: tagName,
              id: current.id || null,
              role: role,
            };
            // Don't break, keep searching for role ancestor
          }
          current = current.parentElement;
          depth++;
        }
        // *** END: ADDED CODE FOR ANCESTOR CONTEXT ***

        const elementData = {
          type: tagName,
          text: textContent,
          bbox: {
            x: rect.left,
            y: rect.top,
            width: rect.width,
            height: rect.height,
          },
          attributes: {
            id: element.id || null,
            class_list: Array.from(element.classList),
            name: element.name || null,
            value: element.value === undefined ? null : element.value,
            placeholder: element.getAttribute("placeholder"),
            type: element.type || null,
            title: element.getAttribute("title"),
            role: role, // Use role variable from earlier
            href: element.getAttribute("href"),
            // Prefer element.dataset for cleaner access to data-* attributes
            data_attributes: Object.fromEntries(
              Object.entries(element.dataset || {}).map(([key, value]) => [
                key === "testid" ? "testid" : key, // Simplify testid key if necessary
                value,
              ])
            ),
          },
          accessibility: {
            role: role, // Use role variable from earlier
            name: accessibleName,
            description: element.getAttribute("aria-description"),
            haspopup: element.getAttribute("aria-haspopup"),
            current: element.getAttribute("aria-current"),
            expanded: element.getAttribute("aria-expanded"),
            selected: element.getAttribute("aria-selected"),
            level: element.getAttribute("aria-level"),
            aria_label: element.getAttribute("aria-label"), // Include direct aria-label
          },
          state: {
            is_visible: true,
            is_enabled: isEnabled, // Use calculated isEnabled
            is_focused: document.activeElement === element,
            is_checked: element.checked === undefined ? null : element.checked,
            is_required:
              element.required === undefined ? null : element.required,
            cursor: style.cursor,
          },
          locators: { label_text: labelText },
          // *** BEGIN: ADDED context KEY ***
          context: {
            parent: parentInfo,
            ancestor: ancestorInfo,
          },
          // *** END: ADDED context KEY ***
        };

        // --- 5. Deduplication ---
        // Use a combination of factors for key to handle similar elements
        const primaryIdentifier =
          elementData.accessibility.name ||
          elementData.text ||
          elementData.attributes.id ||
          `tag:${elementData.type}`;
        // Add position to key for further uniqueness
        const key = `${primaryIdentifier}_${Math.round(
          elementData.bbox.x
        )}_${Math.round(elementData.bbox.y)}`;

        if (!uniqueElementsMap.has(key)) {
          uniqueElementsMap.set(key, elementData);
        } else {
          // Optional: Logic to merge or prioritize if duplicate key found,
          // e.g., prefer element with stronger accessible name or role.
          // For now, just keep the first one encountered.
        }
      } catch (elementError) {
        console.warn("Error processing element:", element, elementError);
      }
    } // End of element loop

    const processingEndTime = performance.now();
    console.log(
      `Processed ${foundElements.length} potential elements in ${(
        processingEndTime - processingStartTime
      ).toFixed(2)} ms.`
    );

    // Assign final sequential IDs
    const uniqueElementList = Array.from(uniqueElementsMap.values());
    let finalId = 0;
    uniqueElementList.forEach((el) => {
      el.id = finalId++;
    });

    const endTime = performance.now();
    console.log(
      `Found ${uniqueElementList.length} unique interactive elements in ${(
        endTime - startTime
      ).toFixed(2)} ms.`
    );
    return uniqueElementList;
  }; // End of window.getInteractiveElements

  // --- Bounding Box UI Functions ---
  window.addBoundingBoxes = function (elementData) {
    console.log(
      `[annotations.js] addBoundingBoxes called with ${
        elementData ? elementData.length : "null"
      } elements.`
    );
    if (!Array.isArray(elementData) || elementData.length === 0) {
      console.log(
        "[annotations.js] No valid element data provided to addBoundingBoxes."
      );
      return;
    }
    window.removeAllBoundingBoxes();
    const fragment = document.createDocumentFragment();
    let boxesAdded = 0;
    elementData.forEach((element) => {
      if (!element || !element.bbox || typeof element.id === "undefined") {
        console.warn(
          "[annotations.js] Skipping element due to missing bbox or id:",
          element
        );
        return;
      }
      const overlay = document.createElement("div");
      overlay.className = "uia-bounding-box-overlay";
      overlay.dataset.uiaElementId = element.id;
      const tooltipLines = [
        `ID: ${element.id}`,
        `Type: ${element.type}${
          element.attributes && element.attributes.role
            ? ` (role: ${element.attributes.role})`
            : ""
        }`,
        `Name: ${
          (element.accessibility && element.accessibility.name) || "(no name)"
        }`,
        element.text &&
        element.accessibility &&
        element.text !== element.accessibility.name
          ? `Text: ${element.text}`
          : null,
        element.attributes && element.attributes.value
          ? `Value: ${element.attributes.value}`
          : null,
        element.attributes && element.attributes.id
          ? `Attr ID: ${element.attributes.id}`
          : null,
        element.attributes && element.attributes.name
          ? `Attr Name: ${element.attributes.name}`
          : null,
        element.attributes && element.attributes.placeholder
          ? `Placeholder: ${element.attributes.placeholder}`
          : null,
        element.attributes && element.attributes.title
          ? `Title: ${element.attributes.title}`
          : null,
        `Enabled: ${
          element.state && typeof element.state.is_enabled !== "undefined"
            ? element.state.is_enabled
            : "N/A"
        }`,
        element.state && element.state.is_checked !== null
          ? `Checked: ${element.state.is_checked}`
          : null,
        element.state && element.state.is_focused
          ? `Focused: ${element.state.is_focused}`
          : null,
        element.locators && element.locators.label_text
          ? `Label: ${element.locators.label_text}`
          : null,
      ].filter((line) => line !== null);
      overlay.setAttribute("title", tooltipLines.join("\n"));
      overlay.style.cssText = `position: fixed; border: 2px solid #2E8B57; background-color: rgba(46, 139, 87, 0.1); box-sizing: border-box; pointer-events: none; z-index: 2147483640; left: ${element.bbox.x}px; top: ${element.bbox.y}px; width: ${element.bbox.width}px; height: ${element.bbox.height}px;`;
      const label = document.createElement("div");
      label.className = "uia-bounding-box-label";
      label.textContent = `${element.id}`;
      label.style.cssText = `position: absolute; top: -1px; left: -1px; background-color: #2E8B57; color: white; padding: 1px 4px; font-size: 10px; font-weight: bold; font-family: sans-serif; border-radius: 2px; white-space: nowrap; opacity: 0.9; min-width: 14px; text-align: center; line-height: 1.2;`;
      overlay.appendChild(label);
      fragment.appendChild(overlay);
      boxesAdded++;
    });
    if (fragment.hasChildNodes()) {
      document.body.appendChild(fragment);
      console.log(
        `[annotations.js] Appended ${boxesAdded} bounding boxes to the document body.`
      );
    } else {
      console.log("[annotations.js] No bounding boxes were created to append.");
    }
  };

  window.removeAllBoundingBoxes = function () {
    const overlays = document.querySelectorAll(".uia-bounding-box-overlay");
    if (overlays.length > 0) {
      console.log(
        `[annotations.js] Removing ${overlays.length} bounding boxes.`
      );
    }
    overlays.forEach((overlay) => overlay.remove());
  };

  // --- Initialization Message ---
  console.log(
    "annotations.js (property collection version - datepicker fix) loaded and ready."
  );
})();
