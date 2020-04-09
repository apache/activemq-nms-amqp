# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

$pkgname = "Apache.NMS.AMQP"
$pkgver = "1.8.2"
$frameworks = "netstandard2.0"

write-progress "Creating package directory." "Initializing..."
if (!(test-path package)) {
    mkdir package
}
else {
    # Clean package content if exists
    Remove-Item package\* -Recurse
}

if (test-path build) {
    Push-Location build

    $pkgdir = "..\package"

    write-progress "Packaging Application files." "Scanning..."
    $zipfile = "$pkgdir\$pkgname-$pkgver-bin.zip"

    Compress-Archive -Path ..\LICENSE.txt, ..\NOTICE.txt -Update -DestinationPath $zipfile
    
    # clean up temp
    Remove-Item temp -Recurse -ErrorAction Ignore

    foreach ($framework in $frameworks) {
        Copy-Item $framework -Destination temp\$framework -Recurse
        Compress-Archive -Path "temp\$framework" -Update -DestinationPath $zipfile
    }
    
    $nupkg = "$pkgname.$pkgver.nupkg"
    $nupkgdestination = "$pkgdir\$nupkg"
    Copy-Item -Path $nupkg -Destination $nupkgdestination

    $snupkg = "$pkgname.$pkgver.snupkg"
    $snupkgdestination = "$pkgdir\$snupkg"
    Copy-Item -Path $snupkg -Destination $snupkgdestination

    # clean up temp
    Remove-Item temp -Recurse -ErrorAction Inquire

    Pop-Location
}

write-progress "Packaging Source code files." "Scanning..."
$pkgdir = "package"
$zipfile = "$pkgdir\$pkgname-$pkgver-src.zip"

# clean temp dir if exists
Remove-Item temp -Recurse -ErrorAction Ignore

# copy files to temp dir
Copy-Item src -Destination temp\src -Recurse
Copy-Item test -Destination temp\test -Recurse

# clean up debug artifacts if there are any
Get-ChildItem temp -Include bin, obj -Recurse | Remove-Item -Recurse

Compress-Archive -Path temp\*, LICENSE.txt, NOTICE.txt, README.md, apache-nms-amqp.sln, package.ps1 -Update -DestinationPath $zipfile

write-progress "Removing temp files"
Remove-Item temp -Recurse

write-progress -Completed "Packaging" "Complete."
