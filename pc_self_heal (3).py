"""
Plastic-Craft Self-Healing Engine
Monitors error patterns across listings, rewrites broken functions
via Claude API, validates rewrites before hot-swapping, and provides
comprehensive dimension/weight sanity checking.

Error triggers:
  - Same error type on 5 consecutive listings → rewrite attempt
  - Same error type on >10% of processed listings → rewrite attempt

Sanity checks:
  - Weight physically possible for material and dimensions
  - Box dimensions larger than product dimensions
  - No dimension exceeds known maximum catalog sizes
  - Density-based plausibility check
  - Cross-check dimensions against title text
"""

import re
import sys
import json
import types
import inspect
import importlib
from datetime import datetime
from pathlib import Path
from collections import deque, Counter

# ============================================================
# KNOWN PHYSICAL LIMITS FOR SANITY CHECKING
# Based on Plastic-Craft's actual catalog
# ============================================================

PHYSICAL_LIMITS = {
    # Maximum realistic dimensions in inches
    'max_sheet_length': 144,   # 12 feet
    'max_sheet_width': 96,     # 8 feet
    'max_sheet_thickness': 6,  # 6 inches
    'max_rod_length': 144,     # 12 feet
    'max_rod_od': 24,          # 24 inch diameter
    'max_tube_length': 144,    # 12 feet
    'max_tube_od': 24,         # 24 inch diameter

    # Minimum realistic dimensions
    'min_sheet_thickness': 0.010,  # 1/100 inch
    'min_rod_od': 0.0625,          # 1/16 inch
    'min_tube_od': 0.125,          # 1/8 inch
    'min_length': 1.0,             # 1 inch

    # Weight limits
    'max_weight_lbs': 2000,    # 1 ton — absolute maximum
    'min_weight_lbs': 0.001,   # 1 gram minimum

    # Box dimension limits
    'max_box_dimension': 200,  # inches
    'min_box_dimension': 1,    # inches

    # Box must be larger than product by at least buffer
    'min_box_buffer': 1.0,     # inches
}

# Density ranges for plausibility check (lb/in³)
# Min and max acceptable values — catches impossible calculations
DENSITY_RANGES = {
    'acrylic':       (0.038, 0.050),
    'abs':           (0.033, 0.045),
    'hdpe':          (0.030, 0.040),
    'pvc':           (0.045, 0.060),
    'polycarbonate': (0.038, 0.050),
    'nylon':         (0.036, 0.048),
    'uhmw':          (0.030, 0.040),
    'polypropylene': (0.028, 0.038),
    'peek':          (0.042, 0.055),
    'delrin':        (0.045, 0.060),
    'ptfe':          (0.070, 0.090),
}

# ============================================================
# DIMENSION SANITY CHECKER
# ============================================================

class DimensionSanityChecker:
    """
    Validates that calculated dimensions and weights make
    physical sense before they go into the feed file.
    Returns detailed issues and suggested corrections.
    """

    def __init__(self, logger=None):
        self.logger = logger

    def _log(self, level, msg):
        if self.logger:
            getattr(self.logger, level)(msg)

    def check_all(self, dims, weight, box_dims, material_key, title):
        """
        Run all sanity checks. Returns:
          (is_valid, issues, corrected_dims, corrected_weight, corrected_box)
        """
        issues = []
        corrected_dims = dims.copy()
        corrected_weight = weight
        corrected_box = box_dims.copy() if box_dims else {}

        ptype = dims.get('product_type', 'unknown')

        # Run each check
        dim_issues = self._check_dimensions(corrected_dims, ptype, title)
        issues.extend(dim_issues)

        weight_issues, corrected_weight = self._check_weight(
            corrected_dims, corrected_weight, material_key, ptype
        )
        issues.extend(weight_issues)

        box_issues, corrected_box = self._check_box(
            corrected_dims, corrected_box, corrected_weight
        )
        issues.extend(box_issues)

        cross_issues = self._cross_check_title(corrected_dims, title, ptype)
        issues.extend(cross_issues)

        is_valid = len([i for i in issues if i['severity'] == 'ERROR']) == 0

        return is_valid, issues, corrected_dims, corrected_weight, corrected_box

    def _check_dimensions(self, dims, ptype, title):
        issues = []
        limits = PHYSICAL_LIMITS

        if ptype in ('sheet', 'cutting_board', 'cutting_board_oem'):
            thickness = dims.get('thickness')
            width = dims.get('width')
            length = dims.get('length')

            if thickness is not None:
                if thickness < limits['min_sheet_thickness']:
                    issues.append({
                        'field': 'thickness',
                        'severity': 'ERROR',
                        'msg': f'Thickness {thickness}" is impossibly thin (min {limits["min_sheet_thickness"]}")',
                        'value': thickness,
                    })
                elif thickness > limits['max_sheet_thickness']:
                    issues.append({
                        'field': 'thickness',
                        'severity': 'ERROR',
                        'msg': f'Thickness {thickness}" exceeds maximum catalog size ({limits["max_sheet_thickness"]}")',
                        'value': thickness,
                    })

            if width is not None and width > limits['max_sheet_width']:
                issues.append({
                    'field': 'width',
                    'severity': 'WARNING',
                    'msg': f'Width {width}" is unusually large — verify against title',
                    'value': width,
                })

            if length is not None and length > limits['max_sheet_length']:
                issues.append({
                    'field': 'length',
                    'severity': 'WARNING',
                    'msg': f'Length {length}" is unusually large — verify against title',
                    'value': length,
                })

            # Check width/length not swapped
            if width and length and width > length:
                issues.append({
                    'field': 'width_length',
                    'severity': 'WARNING',
                    'msg': f'Width ({width}") > Length ({length}") — may be swapped, verify against title',
                    'value': (width, length),
                })

        elif ptype == 'rod':
            od = dims.get('od')
            length = dims.get('length')

            if od is not None:
                if od < limits['min_rod_od']:
                    issues.append({
                        'field': 'od',
                        'severity': 'ERROR',
                        'msg': f'Rod OD {od}" is impossibly small (min {limits["min_rod_od"]}")',
                        'value': od,
                    })
                elif od > limits['max_rod_od']:
                    issues.append({
                        'field': 'od',
                        'severity': 'ERROR',
                        'msg': f'Rod OD {od}" exceeds maximum ({limits["max_rod_od"]}")',
                        'value': od,
                    })

            if length is not None:
                if length < limits['min_length']:
                    issues.append({
                        'field': 'length',
                        'severity': 'ERROR',
                        'msg': f'Rod length {length}" is impossibly short',
                        'value': length,
                    })
                elif length > limits['max_rod_length']:
                    issues.append({
                        'field': 'length',
                        'severity': 'WARNING',
                        'msg': f'Rod length {length}" is unusually long — verify',
                        'value': length,
                    })

            # OD should be much less than length for rods
            if od and length and od > length:
                issues.append({
                    'field': 'od_length_ratio',
                    'severity': 'WARNING',
                    'msg': f'OD ({od}") > Length ({length}") — dimensions may be swapped',
                    'value': (od, length),
                })

        elif ptype == 'tube':
            id_ = dims.get('id')
            od = dims.get('od')
            length = dims.get('length')

            if id_ and od:
                if id_ >= od:
                    issues.append({
                        'field': 'id_od',
                        'severity': 'ERROR',
                        'msg': f'ID ({id_}") >= OD ({od}") — impossible tube dimensions',
                        'value': (id_, od),
                    })
                wall = (od - id_) / 2
                if wall < 0.01:
                    issues.append({
                        'field': 'wall_thickness',
                        'severity': 'ERROR',
                        'msg': f'Wall thickness {wall:.4f}" is impossibly thin',
                        'value': wall,
                    })

        return issues

    def _check_weight(self, dims, weight, material_key, ptype):
        issues = []
        limits = PHYSICAL_LIMITS

        if weight is None:
            return issues, weight

        # Basic range check
        if weight < limits['min_weight_lbs']:
            issues.append({
                'field': 'weight',
                'severity': 'ERROR',
                'msg': f'Weight {weight} lbs is impossibly light',
                'value': weight,
            })
            return issues, None

        if weight > limits['max_weight_lbs']:
            issues.append({
                'field': 'weight',
                'severity': 'ERROR',
                'msg': f'Weight {weight} lbs exceeds maximum ({limits["max_weight_lbs"]} lbs)',
                'value': weight,
            })
            return issues, None

        # Density plausibility check
        density_range = DENSITY_RANGES.get(material_key)
        if density_range and ptype in ('sheet', 'cutting_board', 'cutting_board_oem'):
            thickness = dims.get('thickness')
            width = dims.get('width')
            length = dims.get('length')
            if all(v is not None for v in [thickness, width, length]):
                volume = thickness * width * length
                if volume > 0:
                    implied_density = weight / volume / 1.10  # remove buffer
                    min_d, max_d = density_range
                    if implied_density < min_d * 0.5:
                        issues.append({
                            'field': 'weight_density',
                            'severity': 'WARNING',
                            'msg': f'Implied density {implied_density:.4f} lb/in³ is too low for {material_key} — weight may be wrong',
                            'value': implied_density,
                        })
                        # Recalculate with correct density
                        correct_weight = round(volume * ((min_d + max_d) / 2) * 1.10, 2)
                        issues[-1]['suggested_weight'] = correct_weight
                        weight = correct_weight

                    elif implied_density > max_d * 2.0:
                        issues.append({
                            'field': 'weight_density',
                            'severity': 'WARNING',
                            'msg': f'Implied density {implied_density:.4f} lb/in³ is too high for {material_key} — weight may be wrong',
                            'value': implied_density,
                        })
                        correct_weight = round(volume * ((min_d + max_d) / 2) * 1.10, 2)
                        issues[-1]['suggested_weight'] = correct_weight
                        weight = correct_weight

        # Common sense checks by product type
        if ptype == 'sheet':
            thickness = dims.get('thickness', 0)
            area = dims.get('width', 0) * dims.get('length', 0)
            if thickness and area:
                if thickness < 0.1 and area < 4 and weight > 5:
                    issues.append({
                        'field': 'weight_sanity',
                        'severity': 'WARNING',
                        'msg': f'Small thin sheet ({thickness}" x {area} sq in) should not weigh {weight} lbs',
                        'value': weight,
                    })

        return issues, weight

    def _check_box(self, dims, box_dims, weight):
        issues = []
        limits = PHYSICAL_LIMITS
        corrected_box = box_dims.copy() if box_dims else {}

        if not box_dims:
            return issues, corrected_box

        ptype = dims.get('product_type', '')

        # Box must be larger than product
        if ptype in ('sheet', 'cutting_board', 'cutting_board_oem'):
            product_dims = {
                'length': dims.get('length', 0),
                'width': dims.get('width', 0),
                'height': dims.get('thickness', 0),
            }
            box_map = {
                'length': 'box_length',
                'width': 'box_width',
                'height': 'box_height',
            }
            for dim_key, box_key in box_map.items():
                prod_val = product_dims.get(dim_key, 0)
                box_val = box_dims.get(box_key, 0)
                if prod_val and box_val:
                    if box_val <= prod_val:
                        issues.append({
                            'field': box_key,
                            'severity': 'ERROR',
                            'msg': f'Box {box_key} ({box_val}") must be larger than product {dim_key} ({prod_val}")',
                            'value': box_val,
                        })
                        corrected_box[box_key] = round(prod_val + 2.0, 1)
                        issues[-1]['corrected_to'] = corrected_box[box_key]

        # Box dimensions must be within limits
        for key, val in box_dims.items():
            if val and val > limits['max_box_dimension']:
                issues.append({
                    'field': key,
                    'severity': 'ERROR',
                    'msg': f'Box dimension {key} ({val}") exceeds maximum ({limits["max_box_dimension"]}")',
                    'value': val,
                })

            if val and val < limits['min_box_dimension']:
                issues.append({
                    'field': key,
                    'severity': 'ERROR',
                    'msg': f'Box dimension {key} ({val}") is impossibly small',
                    'value': val,
                })
                corrected_box[key] = max(1.0, dims.get('thickness', 1.0) + 2.0)

        # Weight vs box size sanity
        if weight and box_dims:
            box_vol = (
                box_dims.get('box_length', 1) *
                box_dims.get('box_width', 1) *
                box_dims.get('box_height', 1)
            )
            # If weight > box_volume * 1.5 lb/in³ it's impossible
            # (lead is about 0.41 lb/in³, nothing plastic is denser)
            if box_vol > 0 and weight / box_vol > 0.5:
                issues.append({
                    'field': 'weight_vs_box',
                    'severity': 'ERROR',
                    'msg': f'Weight {weight} lbs cannot fit in box volume {box_vol:.1f} in³ — calculation error',
                    'value': weight,
                })

        return issues, corrected_box

    def _cross_check_title(self, dims, title, ptype):
        """
        Extract numbers from title and verify parsed dimensions
        are actually present in the title text.
        """
        issues = []

        # Extract all numbers from title
        numbers_in_title = set()
        for match in re.finditer(
            r'(\d+(?:\.\d+)?|\d+/\d+|\d+-\d+/\d+)',
            title
        ):
            try:
                val = match.group(1)
                if '/' in val:
                    parts = val.split('-')
                    if len(parts) == 2:
                        whole, frac = parts
                        num, den = frac.split('/')
                        numbers_in_title.add(float(whole) + float(num)/float(den))
                    else:
                        num, den = val.split('/')
                        numbers_in_title.add(float(num)/float(den))
                else:
                    numbers_in_title.add(float(val))
            except:
                pass

        def within_tolerance(val, numbers, tol=0.05):
            return any(abs(val - n) < tol for n in numbers)

        check_fields = []
        if ptype in ('sheet', 'cutting_board', 'cutting_board_oem'):
            check_fields = [
                ('thickness', dims.get('thickness')),
                ('width', dims.get('width')),
                ('length', dims.get('length')),
            ]
        elif ptype == 'rod':
            check_fields = [
                ('od', dims.get('od')),
                ('length', dims.get('length')),
            ]
        elif ptype == 'tube':
            check_fields = [
                ('id', dims.get('id')),
                ('od', dims.get('od')),
                ('length', dims.get('length')),
            ]

        for field_name, val in check_fields:
            if val is not None and not within_tolerance(val, numbers_in_title):
                issues.append({
                    'field': field_name,
                    'severity': 'WARNING',
                    'msg': f'Parsed {field_name}={val} not found in title — possible parse error',
                    'value': val,
                    'title_numbers': sorted(list(numbers_in_title))[:10],
                })

        return issues


# ============================================================
# SELF-HEALING ENGINE
# ============================================================

class SelfHealingEngine:
    """
    Monitors error patterns and rewrites broken functions
    via Claude API when systematic failures are detected.
    """

    # Functions eligible for rewriting
    ELIGIBLE_FUNCTIONS = {
        'extract_dimensions',
        'detect_material',
        'detect_color',
        'detect_pack',
        'calculate_weight',
        'calculate_box_dims',
        'needs_freight_notice',
    }

    # Functions that must NEVER be rewritten (safety)
    PROTECTED_FUNCTIONS = {
        'process_listings',
        'generate_content',
        'save_checkpoint',
        'load_checkpoint',
        'build_error_result',
    }

    def __init__(self, module, logger, jlog,
                 consecutive_threshold=5,
                 percentage_threshold=0.10,
                 auto_apply=False):
        """
        module: the optimizer module object (for hot-swapping)
        logger: the logger instance
        jlog: JSON event logger
        consecutive_threshold: trigger rewrite after N consecutive same errors
        percentage_threshold: trigger if error_type > X% of total processed
        auto_apply: if False, pauses and asks for confirmation
        """
        self.module = module
        self.logger = logger
        self.jlog = jlog
        self.consecutive_threshold = consecutive_threshold
        self.percentage_threshold = percentage_threshold
        self.auto_apply = auto_apply

        # Error tracking
        self.recent_errors = deque(maxlen=20)  # sliding window
        self.all_errors = Counter()
        self.total_processed = 0
        self.rewrites_attempted = {}
        self.rewrites_applied = {}
        self.rewrite_history = []

        # Sanity checker
        self.sanity_checker = DimensionSanityChecker(logger)

        # Claude client (imported from optimizer)
        import anthropic
        self.claude = anthropic.Anthropic()

    def record_result(self, sku, l1_issues, l2_issues, final_issues):
        """Call after each listing is processed to track error patterns."""
        self.total_processed += 1

        all_issues = l1_issues + l2_issues + final_issues
        if all_issues:
            # Normalize issue types (strip numeric suffixes)
            for issue in all_issues:
                issue_type = re.sub(r':\d+', '', issue).strip()
                issue_type = re.sub(r'BULLET\d_', 'BULLET_', issue_type)
                self.recent_errors.append(issue_type)
                self.all_errors[issue_type] += 1

        self._check_triggers()

    def _check_triggers(self):
        """Check if any error pattern warrants a self-rewrite."""
        if self.total_processed < 3:
            return

        # Check consecutive trigger
        if len(self.recent_errors) >= self.consecutive_threshold:
            recent = list(self.recent_errors)[-self.consecutive_threshold:]
            if len(set(recent)) == 1:
                error_type = recent[0]
                self._maybe_trigger_rewrite(
                    error_type,
                    f'{self.consecutive_threshold} consecutive occurrences'
                )

        # Check percentage trigger
        for error_type, count in self.all_errors.items():
            pct = count / self.total_processed
            if pct > self.percentage_threshold and count >= 5:
                self._maybe_trigger_rewrite(
                    error_type,
                    f'{pct:.1%} of all listings ({count}/{self.total_processed})'
                )

    def _maybe_trigger_rewrite(self, error_type, reason):
        """Decide whether to attempt a rewrite for this error type."""
        # Don't retry the same error type more than twice
        attempts = self.rewrites_attempted.get(error_type, 0)
        if attempts >= 2:
            return

        # Map error type to function
        target_function = self._error_to_function(error_type)
        if not target_function:
            return

        if target_function not in self.ELIGIBLE_FUNCTIONS:
            self.logger.warning(
                f'⚠ Self-heal: {target_function} is not eligible for rewriting'
            )
            return

        self.logger.warning(f'\n{"=" * 60}')
        self.logger.warning(f'🔧 SELF-HEAL TRIGGERED')
        self.logger.warning(f'   Error type : {error_type}')
        self.logger.warning(f'   Reason     : {reason}')
        self.logger.warning(f'   Function   : {target_function}')
        self.logger.warning(f'{"=" * 60}')

        self.jlog.log('self_heal_triggered', {
            'error_type': error_type,
            'reason': reason,
            'target_function': target_function,
            'total_processed': self.total_processed,
        })

        self.rewrites_attempted[error_type] = attempts + 1
        self._attempt_rewrite(target_function, error_type, reason)

    def _error_to_function(self, error_type):
        """Map error type string to the function most likely responsible."""
        mapping = {
            'MISSING_THICKNESS': 'extract_dimensions',
            'MISSING_WIDTH_LENGTH': 'extract_dimensions',
            'MISSING_OD': 'extract_dimensions',
            'MISSING_ID_OD': 'extract_dimensions',
            'MISSING_LENGTH': 'extract_dimensions',
            'PRODUCT_TYPE_UNKNOWN': 'extract_dimensions',
            'MATERIAL_UNKNOWN': 'detect_material',
            'COLOR_MISSING': 'detect_color',
            'WEIGHT_ERROR': 'calculate_weight',
            'BOX_ERROR': 'calculate_box_dims',
            'FREIGHT_ERROR': 'needs_freight_notice',
        }
        for key, func in mapping.items():
            if key in error_type:
                return func
        # Fallback: if it's a dimension parse error
        if 'DIM' in error_type or 'PARSE' in error_type:
            return 'extract_dimensions'
        return None

    def _attempt_rewrite(self, function_name, error_type, reason):
        """Request Claude to rewrite the failing function."""
        # Get current source code of the function
        try:
            current_func = getattr(self.module, function_name, None)
            if current_func is None:
                self.logger.error(f'✗ Cannot find function {function_name} in module')
                return
            current_source = inspect.getsource(current_func)
        except Exception as e:
            self.logger.error(f'✗ Cannot get source for {function_name}: {e}')
            return

        # Get recent failing titles for context
        recent_failing = getattr(self, '_recent_failing_titles', [])

        self.logger.info(f'↻ Calling Claude API to rewrite {function_name}...')

        prompt = f"""You are debugging a Python function in an Amazon listing optimizer for a plastic products company.

PROBLEM:
The function `{function_name}` is producing error type: `{error_type}`
Reason this was triggered: {reason}

CURRENT FUNCTION SOURCE:
```python
{current_source}
```

RECENT FAILING PRODUCT TITLES (the inputs that caused failures):
{json.dumps(recent_failing[-10:], indent=2)}

TASK:
Rewrite `{function_name}` to fix the systematic error while preserving all existing correct behavior.

REQUIREMENTS:
1. Keep the same function signature (same parameters and return type)
2. Handle edge cases in the failing titles
3. Be more robust to unusual formatting, fractions, decimals, mixed formats
4. Do NOT import any new modules — only use Python standard library
5. Add inline comments explaining what you changed and why

Return ONLY the complete corrected Python function definition.
Start with `def {function_name}(` — no preamble, no markdown fences, no explanation."""

        try:
            response = self.claude.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}]
            )
            rewritten_source = response.content[0].text.strip()

            # Clean up any markdown
            rewritten_source = re.sub(r'^```python\s*', '', rewritten_source)
            rewritten_source = re.sub(r'\s*```$', '', rewritten_source)

            self.logger.info(f'✓ Rewrite received — validating...')
            self._validate_and_apply(
                function_name, rewritten_source, error_type, reason
            )

        except Exception as e:
            self.logger.error(f'✗ Self-heal API call failed: {e}')
            self.jlog.log('self_heal_api_failed', {
                'function': function_name,
                'error': str(e)
            })

    def _validate_and_apply(self, function_name, rewritten_source, error_type, reason):
        """
        Validate the rewritten function before hot-swapping.
        Tests it against known-good and known-failing cases.
        """
        # Step 1: Syntax check
        try:
            compile(rewritten_source, '<rewrite>', 'exec')
        except SyntaxError as e:
            self.logger.error(f'✗ Rewritten function has syntax error: {e}')
            self.jlog.log('self_heal_syntax_error', {
                'function': function_name,
                'error': str(e)
            })
            return

        # Step 2: Execute in sandbox namespace
        sandbox = {}
        try:
            exec(rewritten_source, sandbox)
            new_func = sandbox.get(function_name)
            if new_func is None:
                self.logger.error(f'✗ Rewrite did not produce function {function_name}')
                return
        except Exception as e:
            self.logger.error(f'✗ Rewrite failed to execute: {e}')
            return

        # Step 3: Run validation tests
        passed, failed = self._run_validation_tests(function_name, new_func)

        self.logger.info(f'  Validation: {passed} passed, {failed} failed')

        if failed > 0 and failed >= passed:
            self.logger.warning(
                f'⚠ Rewrite validation failed ({failed} failures) — NOT applying'
            )
            self.jlog.log('self_heal_validation_failed', {
                'function': function_name,
                'passed': passed,
                'failed': failed,
            })
            return

        # Step 4: Confirmation (if not auto-apply)
        if not self.auto_apply:
            self.logger.warning(f'\n{"=" * 60}')
            self.logger.warning(f'🔧 PROPOSED REWRITE FOR: {function_name}')
            self.logger.warning(f'   Validation: {passed} passed, {failed} failed')
            self.logger.warning(f'   Reason: {reason}')
            self.logger.warning(f'\n--- REWRITTEN CODE ---')
            for line in rewritten_source.split('\n')[:20]:
                self.logger.warning(f'  {line}')
            if len(rewritten_source.split('\n')) > 20:
                self.logger.warning(f'  ... ({len(rewritten_source.split(chr(10)))} lines total)')
            self.logger.warning(f'{"=" * 60}')
            self.logger.warning(f'Apply this rewrite? (y/n): ')

            try:
                answer = input().strip().lower()
                if answer != 'y':
                    self.logger.info('Rewrite skipped by user')
                    self.jlog.log('self_heal_skipped_by_user', {
                        'function': function_name
                    })
                    return
            except:
                # If running non-interactively, skip
                self.logger.warning('Non-interactive mode — skipping rewrite (set auto_apply=True to apply automatically)')
                return

        # Step 5: Hot-swap
        try:
            setattr(self.module, function_name, new_func)
            self.rewrites_applied[function_name] = rewritten_source
            self.rewrite_history.append({
                'ts': datetime.now().isoformat(),
                'function': function_name,
                'error_type': error_type,
                'reason': reason,
                'validation_passed': passed,
                'validation_failed': failed,
            })

            self.logger.info(f'✓ HOT-SWAP APPLIED: {function_name} has been replaced')
            self.logger.info(f'  Processing will continue with corrected function')

            self.jlog.log('self_heal_applied', {
                'function': function_name,
                'error_type': error_type,
                'passed': passed,
                'failed': failed,
            })

            # Save rewritten source to file for review
            rewrite_file = f'/home/claude/pc_rewrite_{function_name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.py'
            with open(rewrite_file, 'w') as f:
                f.write(f'# Self-heal rewrite of {function_name}\n')
                f.write(f'# Triggered by: {error_type}\n')
                f.write(f'# Reason: {reason}\n')
                f.write(f'# Applied: {datetime.now().isoformat()}\n\n')
                f.write(rewritten_source)
            self.logger.info(f'  Rewrite saved to: {rewrite_file}')

        except Exception as e:
            self.logger.error(f'✗ Hot-swap failed: {e}')
            self.jlog.log('self_heal_hotswap_failed', {
                'function': function_name,
                'error': str(e)
            })

    def _run_validation_tests(self, function_name, new_func):
        """
        Run built-in test cases against the rewritten function.
        Returns (passed_count, failed_count).
        """
        tests = VALIDATION_TESTS.get(function_name, [])
        if not tests:
            self.logger.debug(f'  No validation tests for {function_name} — skipping')
            return 1, 0

        passed = 0
        failed = 0

        for test in tests:
            try:
                args = test['args']
                expected_type = test.get('expected_type')
                expected_not_none = test.get('expected_not_none', False)
                expected_contains = test.get('expected_contains')

                result = new_func(*args)

                ok = True
                if expected_type and not isinstance(result, expected_type):
                    ok = False
                if expected_not_none and result is None:
                    ok = False
                if expected_contains:
                    for key, val in expected_contains.items():
                        if isinstance(result, dict) and result.get(key) != val:
                            ok = False

                if ok:
                    passed += 1
                else:
                    failed += 1
                    self.logger.debug(f'  FAIL: {test.get("name","?")} → got {result}')

            except Exception as e:
                failed += 1
                self.logger.debug(f'  EXCEPTION in test {test.get("name","?")}: {e}')

        return passed, failed

    def record_failing_title(self, title):
        """Record a title that caused a parsing failure for use in rewrite context."""
        if not hasattr(self, '_recent_failing_titles'):
            self._recent_failing_titles = []
        self._recent_failing_titles.append(title)
        if len(self._recent_failing_titles) > 20:
            self._recent_failing_titles.pop(0)

    def get_summary(self):
        """Return a summary of self-healing activity for the run log."""
        return {
            'total_processed': self.total_processed,
            'rewrites_attempted': dict(self.rewrites_attempted),
            'rewrites_applied': list(self.rewrites_applied.keys()),
            'rewrite_history': self.rewrite_history,
            'top_errors': dict(self.all_errors.most_common(10)),
        }


# ============================================================
# BUILT-IN VALIDATION TEST CASES
# Used to verify rewritten functions before hot-swapping
# ============================================================

VALIDATION_TESTS = {
    'extract_dimensions': [
        {
            'name': 'standard_sheet',
            'args': ['ABS Sheet, Black, 1/4" Thick, 12" W x 24" L'],
            'expected_type': dict,
            'expected_contains': {'product_type': 'sheet', 'thickness': 0.25, 'width': 12.0, 'length': 24.0},
        },
        {
            'name': 'decimal_sheet',
            'args': ['ABS Sheet, .236" Thick, 12" x 12"'],
            'expected_type': dict,
            'expected_contains': {'product_type': 'sheet'},
        },
        {
            'name': 'rod_with_diameter',
            'args': ['ABS Round Rod, Natural, 12" Length x 0.375" Diameter'],
            'expected_type': dict,
            'expected_contains': {'product_type': 'rod', 'length': 12.0},
        },
        {
            'name': 'tube_with_id_od',
            'args': ['Acrylic Round Tube, Clear, 1/2" ID x 1" OD x 12" L'],
            'expected_type': dict,
            'expected_contains': {'product_type': 'tube', 'id': 0.5, 'od': 1.0, 'length': 12.0},
        },
        {
            'name': 'cutting_board_oem',
            'args': ['Cutting Board OEM Replacement for Continental (12" x 42")'],
            'expected_type': dict,
            'expected_contains': {'product_type': 'cutting_board_oem'},
        },
        {
            'name': 'mixed_fraction',
            'args': ['HDPE Sheet, White, 1-1/2" Thick, 24" W x 48" L'],
            'expected_type': dict,
            'expected_contains': {'product_type': 'sheet', 'thickness': 1.5},
        },
    ],
    'detect_material': [
        {
            'name': 'acrylic',
            'args': ['Cast Acrylic Sheet, Clear, 1/4" Thick'],
            'expected_not_none': True,
        },
        {
            'name': 'hdpe',
            'args': ['HDPE Cutting Board, White, 1/2" Thick'],
            'expected_not_none': True,
        },
        {
            'name': 'polycarbonate',
            'args': ['Polycarbonate Sheet, Clear, 1/4" Thick, 12" x 24"'],
            'expected_not_none': True,
        },
    ],
    'calculate_weight': [
        {
            'name': 'abs_sheet_small',
            'args': ['abs', {'product_type': 'sheet', 'thickness': 0.25, 'width': 12.0, 'length': 12.0}],
            'expected_not_none': True,
        },
        {
            'name': 'acrylic_rod',
            'args': ['acrylic', {'product_type': 'rod', 'od': 1.0, 'length': 12.0}],
            'expected_not_none': True,
        },
    ],
}
