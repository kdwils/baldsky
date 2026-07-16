# Unit Testing Conventions

## Structure
- One test function per method/function being tested (e.g., `TestGetFeedPage`)
- Individual `t.Run` subtests within for each test case
- No table-driven tests

## Assertions
- Always assert the full returned response struct (`got` vs `want`), never partial field checks
- For multi-return functions, capture and assert ALL return values, including zero values on error paths
- Use `require` for preconditions that must pass before assertions (`require.NoError`, `require.ErrorIs`)
- Use `assert` for the actual value comparisons (`assert.Equal`, `assert.ElementsMatch`)

## Mocks
- Use `gomock` with generated mocks from `go.uber.org/mock`
- Each `t.Run` subtest creates its own `gomock.Controller` and service instance
- For methods with runtime-generated fields (e.g., timestamps), capture the arg via `gomock.Do` and assert the captured struct after the call
- Prefer explicit `GetFeedPageParams` matchers over `gomock.Any()` when verifying DB query params
