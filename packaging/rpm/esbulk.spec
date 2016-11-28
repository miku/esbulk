Summary:    Fast parallel bulk loading utility for elasticsearch.
Name:       esbulk
Version:    0.4.2
Release:    0
License:    MIT
BuildArch:  x86_64
BuildRoot:  %{_tmppath}/%{name}-build
Group:      System/Base
Vendor:     UB Leipzig
URL:        https://github.com/miku/esbulk

%description

Fast parallel bulk loading utility for elasticsearch.

%prep
# the set up macro unpacks the source bundle and changes in to the represented by
# %{name} which in this case would be my_maintenance_scripts. So your source bundle
# needs to have a top level directory inside called my_maintenance _scripts
# %setup -n %{name}

%build
# this section is empty for this example as we're not actually building anything

%install
# create directories where the files will be located
mkdir -p $RPM_BUILD_ROOT/usr/local/sbin

# put the files in to the relevant directories.
# the argument on -m is the permissions expressed as octal. (See chmod man page for details.)
install -m 755 esbulk $RPM_BUILD_ROOT/usr/local/sbin

%post
# the post section is where you can run commands after the rpm is installed.
# insserv /etc/init.d/my_maintenance

%clean
rm -rf $RPM_BUILD_ROOT
rm -rf %{_tmppath}/%{name}
rm -rf %{_topdir}/BUILD/%{name}

# list files owned by the package here
%files
%defattr(-,root,root)
/usr/local/sbin/esbulk


%changelog
* Mon Nov 28 2016 Martin Czygan
- 0.4.2 release
- support for X-Pack, HTTP Basic AUTH, with curl syntax (esbulk -u username:password)

* Mon Nov 28 2016 Martin Czygan
- 0.4.1 release
- abort indexing, if a single failed bulk request is encountered; do not silently lose documents

* Sat Nov 26 2016 Martin Czygan
- 0.4.0 release
- attempted fix for https://github.com/miku/esbulk/issues/5

* Thu Nov 10 2015 Martin Czygan
- 0.3.5 release
- add -mapping and -purge flags

* Thu May 7 2015 Martin Czygan
- 0.3.3 release
- improve error handling (missing index, wrong index name, ...)

* Mon Dec 1 2014 Martin Czygan
- 0.3.2 release
- fix panics on connection errors

* Sun Nov 30 2014 Martin Czygan
- 0.3.1 release
- fix index.refresh_interval settings, make indexing 10-30% faster

* Sun Nov 30 2014 Martin Czygan
- 0.3 release
- backwards-incompatible changes: removed -q, added -verbose
- added support for gzipped input files

* Mon Sep 29 2014 Martin Czygan
- 0.2 release, fixed memory leak by closing `response.Body`

* Tue Aug 26 2014 Martin Czygan
- 0.1 release
