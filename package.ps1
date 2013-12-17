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
$pkgver = "1.6-SNAPSHOT"
$configurations = "release", "debug"
$frameworks = "net-2.0", "net-4.0"

write-progress "Creating package directory." "Initializing..."
if(!(test-path package))
{
    md package
}

if(test-path build)
{
    pushd build

    $pkgdir = "..\package"

    write-progress "Packaging Application files." "Scanning..."
    $zipfile = "$pkgdir\$pkgname-$pkgver-bin.zip"
    zip -9 -u -j "$zipfile" ..\LICENSE.txt
    zip -9 -u -j "$zipfile" ..\NOTICE.txt
    foreach($configuration in $configurations)
    {
        foreach($framework in $frameworks)
        {
            zip -9 -u "$zipfile" "$framework\$configuration\$pkgname.dll"
            zip -9 -u "$zipfile" "$framework\$configuration\$pkgname.xml"
            zip -9 -u "$zipfile" "$framework\$configuration\nmsprovider*.config"
            zip -9 -u "$zipfile" "$framework\$configuration\$pkgname.Test.dll"
            zip -9 -u "$zipfile" "$framework\$configuration\$pkgname.Test.xml"
            if($framework -ieq "mono-2.0")
            {
                zip -9 -u "$zipfile" "$framework\$configuration\$pkgname.dll.mdb"
                zip -9 -u "$zipfile" "$framework\$configuration\$pkgname.Test.dll.mdb"
            }
            else
            {
                zip -9 -u "$zipfile" "$framework\$configuration\$pkgname.pdb"
                zip -9 -u "$zipfile" "$framework\$configuration\$pkgname.Test.pdb"
            }
        }
    }

    popd
}

write-progress "Packaging Source code files." "Scanning..."
$pkgdir = "package"
$zipfile = "$pkgdir\$pkgname-$pkgver-src.zip"

zip -9 -u "$zipfile" LICENSE.txt NOTICE.txt nant-common.xml nant.build package.ps1 vs2008-amqp-test.csproj vs2008-amqp.csproj vs2008-amqp.sln vs2010-amqp-test.csproj vs2010-amqp.csproj vs2010-amqp.sln nmsprovider*.config
zip -9 -u -r "$zipfile" keyfile src

write-progress -Completed "Packaging" "Complete."
