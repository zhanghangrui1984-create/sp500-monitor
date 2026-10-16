"""Compatibility entry point: the retired ten-trigger engine is never executed."""
from monitor_v17.main import main
if __name__=='__main__':
    raise SystemExit(main())
