'''Unit tests on runner functions'''
import pytest, copy, os

from ricorde.runrs import load_params

@pytest.mark.dev
@pytest.mark.parametrize('param_fn', ['params_default.txt'])
def test_load_params(proj_dir, param_fn):
    fp = os.path.join(proj_dir, param_fn)
    assert os.path.exists(fp)
    load_params(fp)