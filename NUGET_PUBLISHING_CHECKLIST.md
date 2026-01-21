# NuGet Publishing Checklist for AzureDataLakeTools

## Pre-Publishing Checklist

### 1. Code Quality
- [ ] All unit tests pass
- [ ] No compiler warnings (or acceptable warnings documented)
- [ ] Code follows consistent style guidelines
- [ ] XML documentation is complete for all public APIs
- [ ] Sample project builds and runs successfully

### 2. Version Management
- [ ] Update version number in `.csproj` file following semantic versioning
  - Major: Breaking changes
  - Minor: New features (backwards compatible)
  - Patch: Bug fixes
- [ ] Update release notes in `CHANGELOG.md` (create if doesn't exist)
- [ ] Tag the release in git: `git tag v1.0.0.3`

### 3. Package Metadata
- [ ] Verify all NuGet metadata in `.csproj`:
  - PackageId: `AzureDataLakeTools.Storage`
  - Version: Current version
  - Authors: Correct author name
  - Description: Accurate and compelling
  - PackageLicenseExpression: `MIT`
  - PackageProjectUrl: Update from placeholder
  - RepositoryUrl: Update from placeholder
  - PackageTags: Relevant and searchable
- [ ] README.md is included in package
- [ ] LICENSE file is included in package

### 4. Dependencies
- [ ] Review all package dependencies for security vulnerabilities
- [ ] Ensure minimum required versions are specified
- [ ] Remove any unnecessary dependencies

### 5. Build Configuration
- [ ] Build in Release mode: `dotnet build -c Release`
- [ ] Pack the library: `dotnet pack -c Release`
- [ ] Verify package contents: `dotnet nuget locals all --clear`

## Publishing Steps

### 1. Create the Package
```bash
# Clean previous builds
dotnet clean

# Build in Release mode
dotnet build -c Release

# Create NuGet package
dotnet pack -c Release --output ./nupkg

# The package will be in: ./nupkg/AzureDataLakeTools.Storage.1.0.0.3.nupkg
```

### 2. Test the Package Locally
```bash
# Create a test project
mkdir test-package
cd test-package
dotnet new console

# Add local package source
dotnet nuget add source /path/to/nupkg --name LocalTest

# Install the package
dotnet add package AzureDataLakeTools.Storage --version 1.0.0.3 --source LocalTest

# Test basic functionality
```

### 3. Publish to NuGet.org
```bash
# Get your API key from https://www.nuget.org/account/apikeys
# Store it securely (don't commit to source control!)

# Push to NuGet.org
dotnet nuget push ./nupkg/AzureDataLakeTools.Storage.1.0.0.3.nupkg \
  --api-key YOUR_API_KEY \
  --source https://api.nuget.org/v3/index.json

# Or push including symbols
dotnet nuget push ./nupkg/AzureDataLakeTools.Storage.1.0.0.3.nupkg \
  --api-key YOUR_API_KEY \
  --source https://api.nuget.org/v3/index.json \
  --symbol-source https://api.nuget.org/v3/index.json
```

## Post-Publishing

### 1. Verify Package
- [ ] Check package page on NuGet.org
- [ ] Verify all metadata displays correctly
- [ ] Test installation in a fresh project
- [ ] Ensure README renders properly on NuGet.org

### 2. Documentation
- [ ] Update any external documentation
- [ ] Create GitHub release with release notes
- [ ] Update project website (if applicable)

### 3. Communication
- [ ] Announce release (Twitter, blog, etc.)
- [ ] Notify major users of new version
- [ ] Monitor for initial feedback/issues

## CI/CD Recommendations

### GitHub Actions Example
Create `.github/workflows/publish.yml`:

```yaml
name: Publish to NuGet

on:
  release:
    types: [published]

jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3
    - name: Setup .NET
      uses: actions/setup-dotnet@v3
      with:
        dotnet-version: 8.0.x
    - name: Restore dependencies
      run: dotnet restore
    - name: Build
      run: dotnet build -c Release --no-restore
    - name: Test
      run: dotnet test -c Release --no-build
    - name: Pack
      run: dotnet pack -c Release --no-build --output ./nupkg
    - name: Push to NuGet
      run: |
        dotnet nuget push ./nupkg/*.nupkg \
          --api-key ${{ secrets.NUGET_API_KEY }} \
          --source https://api.nuget.org/v3/index.json \
          --skip-duplicate
```

### Azure DevOps Pipeline Example
```yaml
trigger:
  tags:
    include:
    - v*

pool:
  vmImage: 'ubuntu-latest'

variables:
  buildConfiguration: 'Release'

steps:
- task: DotNetCoreCLI@2
  displayName: 'Restore'
  inputs:
    command: 'restore'
    projects: '**/*.csproj'

- task: DotNetCoreCLI@2
  displayName: 'Build'
  inputs:
    command: 'build'
    projects: '**/*.csproj'
    arguments: '--configuration $(buildConfiguration)'

- task: DotNetCoreCLI@2
  displayName: 'Test'
  inputs:
    command: 'test'
    projects: '**/*Tests.csproj'
    arguments: '--configuration $(buildConfiguration)'

- task: DotNetCoreCLI@2
  displayName: 'Pack'
  inputs:
    command: 'pack'
    packagesToPack: '**/AzureDataLakeTools.Storage.csproj'
    configuration: '$(buildConfiguration)'
    outputDir: '$(Build.ArtifactStagingDirectory)'

- task: NuGetCommand@2
  displayName: 'Push to NuGet.org'
  inputs:
    command: 'push'
    packagesToPush: '$(Build.ArtifactStagingDirectory)/**/*.nupkg'
    nuGetFeedType: 'external'
    publishFeedCredentials: 'NuGet.org'
```

## Troubleshooting

### Common Issues

1. **Package ID already exists**
   - Ensure version number is incremented
   - Check for typos in package ID

2. **Missing dependencies**
   - Verify all required packages are referenced
   - Check target framework compatibility

3. **README not showing on NuGet**
   - Ensure `PackageReadmeFile` is set in .csproj
   - Verify README.md is included in package

4. **Symbols package issues**
   - Ensure PDB files are generated
   - Use `snupkg` format for symbol packages

## Security Considerations

1. **API Key Management**
   - Never commit API keys to source control
   - Use environment variables or secure key vaults
   - Rotate keys regularly
   - Use minimal scope keys when possible

2. **Package Signing**
   - Consider signing packages for added security
   - Document signing certificate details

3. **Dependency Security**
   - Regularly update dependencies
   - Monitor for security advisories
   - Use tools like `dotnet list package --vulnerable`

## Next Steps

1. **Update GitHub/Repository URLs**: Replace placeholder URLs in .csproj
2. **Create CHANGELOG.md**: Document version history
3. **Set up CI/CD**: Automate the publishing process
4. **Create API Key**: Get from https://www.nuget.org/account/apikeys
5. **Test Publishing**: Try with a pre-release version first