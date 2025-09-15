if [ $# -ne 1 ]; then
  echo "Usage: $0 <rc-number>"
  exit 1
fi
rc="$1"
cd src
echo "Building venv..."
rm -rf ./buildvenv
python3 -m venv ./buildvenv
source buildvenv/bin/activate
echo "Now acting in venv..."
rm -rf dist/
sed -i.bak -E "s/^version *= *\"[^\"]+\"/version = \"0.1.0rc${rc}\"/" pyproject.toml
echo "Updated pyproject.toml version to 0.1.0rc${rc}"
python3 -m pip install --upgrade build twine
python3 -m build
python3 -m pip install dist/ranchero-0.1.0rc"$rc"-py3-none-any.whl --force-reinstall
cd ..
current_time="$(date +%s)"
python -v -c "import ranchero"
import_time="$(($(date +%s)-current_time))"
echo "${import_time} seconds for first-time import of ranchero"
python3 tests.py
read -p "Looks good?"
python3 standardize_any_bigquery_json.py
read -p "Looks good?"
pip show -f ranchero
read -p "Looks good?"
cd src
tree dist
read -p "Looks good?"
