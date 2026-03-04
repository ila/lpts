#pragma once

// Set to 1 to enable verbose LPTS debug output, 0 to disable
#define LPTS_DEBUG 0

#if LPTS_DEBUG
#define LPTS_DEBUG_PRINT(x) Printer::Print(x)
#else
#define LPTS_DEBUG_PRINT(x) ((void)0)
#endif
