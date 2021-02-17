#include "neo/query/globals.h"


/// costs for max_predicted_time estimations, in nanoseconds
/// YMMV, defaults were estimated in a very specific environment, and then rounded off
int g_iPredictorCostDoc = 64;
int g_iPredictorCostHit = 48;
int g_iPredictorCostSkip = 2048;
int g_iPredictorCostMatch = 64;